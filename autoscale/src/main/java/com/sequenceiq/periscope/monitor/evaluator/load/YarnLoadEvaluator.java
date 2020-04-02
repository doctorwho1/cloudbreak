package com.sequenceiq.periscope.monitor.evaluator.load;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.response.StackV4Response;
import com.sequenceiq.cloudbreak.logger.MDCBuilder;
import com.sequenceiq.periscope.domain.Cluster;
import com.sequenceiq.periscope.domain.LoadAlert;
import com.sequenceiq.periscope.model.yarn.DecommissionCandidate;
import com.sequenceiq.periscope.model.yarn.NewNodeManagerCandidates;
import com.sequenceiq.periscope.model.yarn.YarnScalingServiceV1Response;
import com.sequenceiq.periscope.monitor.client.YarnMetricsClient;
import com.sequenceiq.periscope.monitor.context.ClusterIdEvaluatorContext;
import com.sequenceiq.periscope.monitor.context.EvaluatorContext;
import com.sequenceiq.periscope.monitor.evaluator.EvaluatorExecutor;
import com.sequenceiq.periscope.monitor.evaluator.EventPublisher;
import com.sequenceiq.periscope.monitor.event.ScalingEvent;
import com.sequenceiq.periscope.monitor.event.UpdateFailedEvent;
import com.sequenceiq.periscope.monitor.handler.CloudbreakCommunicator;
import com.sequenceiq.periscope.repository.LoadAlertRepository;
import com.sequenceiq.periscope.service.ClusterService;
import com.sequenceiq.periscope.utils.ClusterUtils;
import com.sequenceiq.periscope.utils.StackResponseUtils;
import com.sequenceiq.periscope.utils.TimeUtil;

@Component("YarnLoadEvaluator")
@Scope("prototype")
public class YarnLoadEvaluator extends EvaluatorExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(YarnLoadEvaluator.class);

    private static final String EVALUATOR_NAME = YarnLoadEvaluator.class.getName();

    @Inject
    private ClusterService clusterService;

    @Inject
    private LoadAlertRepository alertRepository;

    @Inject
    private EventPublisher eventPublisher;

    @Inject
    private YarnMetricsClient yarnMetricsClient;

    @Inject
    private StackResponseUtils stackResponseUtils;

    @Inject
    private CloudbreakCommunicator cloudbreakCommunicator;

    private long clusterId;

    private Cluster cluster;

    private LoadAlert loadAlert;

    private StackV4Response stackV4Response;

    private Map<String, String> hostGroupFqdnToInstanceId;

    @Nonnull
    @Override
    public EvaluatorContext getContext() {
        return new ClusterIdEvaluatorContext(clusterId);
    }

    @Override
    public void setContext(EvaluatorContext context) {
        clusterId = (long) context.getData();
        cluster = clusterService.findById(clusterId);
        loadAlert = cluster.getLoadAlerts().stream().findFirst().get();
        stackV4Response = cloudbreakCommunicator.getByCrn(cluster.getStackCrn());
        hostGroupFqdnToInstanceId = stackResponseUtils
                .getCloudInstanceIdsForHostGroup(stackV4Response, loadAlert.getScalingPolicy().getHostGroup());
    }

    @Override
    public String getName() {
        return EVALUATOR_NAME;
    }

    @Override
    protected void execute() {
        long loadAlertCoolDownMS = loadAlert.getLoadAlertConfiguration().getCoolDownMinutes() * TimeUtil.MIN_IN_MS;
        long remainingTime = ClusterUtils
                .getRemainingCooldownTime(loadAlertCoolDownMS, cluster.getLastScalingActivity());
        if (remainingTime > 0) {
            pollYarnMetricsAndScaleCluster();
        }
    }

    protected void pollYarnMetricsAndScaleCluster() {
        long start = System.currentTimeMillis();
        try {
            MDCBuilder.buildMdcContext(cluster);
            String hostGroupInstanceType =
                    stackResponseUtils.getHostGroupInstanceType(stackV4Response, loadAlert.getScalingPolicy().getHostGroup());

            YarnScalingServiceV1Response yarnResponse = yarnMetricsClient
                    .getYarnMetricsForCluster(cluster, hostGroupInstanceType, stackV4Response.getCloudPlatform());

            yarnResponse.getScaleUpCandidates().ifPresent(
                    scaleUpCandidates -> handleScaleUp(hostGroupInstanceType, scaleUpCandidates)
            );
            yarnResponse.getScaleDownCandidates().ifPresent(
                    scaleDownCandidates -> handleScaleDown(scaleDownCandidates)
            );

            LOGGER.debug("Finished loadEvaluator for cluster {} in {} ms", cluster.getStackCrn(), System.currentTimeMillis() - start);
        } catch (Exception ex) {
            LOGGER.info("Failed to process load alert for Cluster {}, exception {}", cluster.getStackCrn(), ex);
            eventPublisher.publishEvent(new UpdateFailedEvent(clusterId));
        } finally {
            LOGGER.debug("Finished loadEvaluator for cluster {} in {} ms", cluster.getStackCrn(), System.currentTimeMillis() - start);
        }
    }

    protected void handleScaleUp(String hostGroupInstanceType, NewNodeManagerCandidates newNodeManagerCandidates) {
        Integer yarnRecommendedHostGroupCount =
                newNodeManagerCandidates.getCandidates().stream()
                        .filter(candidate -> candidate.getModelName().equalsIgnoreCase(hostGroupInstanceType))
                        .findFirst().map(NewNodeManagerCandidates.Candidate::getCount)
                        .orElseThrow(() -> new RuntimeException(String.format("Yarn Scaling API Response does not contain recommended host count " +
                                        "for hostGroupInstanceType %s in Cluster %s, Yarn Response %s",
                                hostGroupInstanceType, cluster.getStackCrn(), newNodeManagerCandidates)));

        int maxAllowedScaleUp = loadAlert.getLoadAlertConfiguration().getMaxResourceValue() - hostGroupFqdnToInstanceId.size();
        maxAllowedScaleUp = maxAllowedScaleUp < 0 ? 0 : maxAllowedScaleUp;
        int scaleUpCount = Math.min(maxAllowedScaleUp, yarnRecommendedHostGroupCount);

        loadAlert.getScalingPolicy().setScalingAdjustment(scaleUpCount);
        eventPublisher.publishEvent(new ScalingEvent(loadAlert));
    }

    protected void handleScaleDown(List<DecommissionCandidate> decommissionCandidates) {

        int maxAllowedScaleDown = hostGroupFqdnToInstanceId.size() - loadAlert.getLoadAlertConfiguration().getMinResourceValue();
        maxAllowedScaleDown = maxAllowedScaleDown < 0 ? 0 : maxAllowedScaleDown;

        List<String> decommissionHostGroupNodeIds = decommissionCandidates.stream()
                .sorted(Comparator.comparingInt(DecommissionCandidate::getAmCount))
                .map(DecommissionCandidate::getNodeId)
                .map(nodeFqdn -> nodeFqdn.split(":")[0])
                .filter(s -> hostGroupFqdnToInstanceId.keySet().contains(s))
                .limit(maxAllowedScaleDown)
                .map(nodeFqdn -> hostGroupFqdnToInstanceId.get(nodeFqdn))
                .collect(Collectors.toList());

        LOGGER.info(String.format("Decommission Candidates for Cluster %s, HostGroup %s, NodeIds %s",
                cluster.getStackCrn(), loadAlert.getScalingPolicy().getHostGroup(), decommissionHostGroupNodeIds));

        loadAlert.setDecommissionNodeIds(decommissionHostGroupNodeIds);
        eventPublisher.publishEvent(new ScalingEvent(loadAlert));
    }
}
