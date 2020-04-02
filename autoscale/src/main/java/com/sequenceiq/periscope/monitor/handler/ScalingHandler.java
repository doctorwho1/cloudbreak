package com.sequenceiq.periscope.monitor.handler;

import static java.lang.Math.ceil;

import java.util.concurrent.ExecutorService;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.api.endpoint.v4.stacks.response.StackV4Response;
import com.sequenceiq.cloudbreak.logger.MDCBuilder;
import com.sequenceiq.periscope.domain.BaseAlert;
import com.sequenceiq.periscope.domain.Cluster;
import com.sequenceiq.periscope.domain.LoadAlert;
import com.sequenceiq.periscope.domain.ScalingPolicy;
import com.sequenceiq.periscope.monitor.evaluator.cm.ClouderaManagerTotalHostsEvaluator;
import com.sequenceiq.periscope.monitor.event.ScalingEvent;
import com.sequenceiq.periscope.service.ClusterService;
import com.sequenceiq.periscope.service.RejectedThreadService;
import com.sequenceiq.periscope.utils.ClusterUtils;
import com.sequenceiq.periscope.utils.StackResponseUtils;
import com.sequenceiq.periscope.utils.TimeUtil;

@Component
public class ScalingHandler implements ApplicationListener<ScalingEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScalingHandler.class);

    @Inject
    @Qualifier("periscopeListeningScheduledExecutorService")
    private ExecutorService executorService;

    @Inject
    private ClusterService clusterService;

    @Inject
    private ApplicationContext applicationContext;

    @Inject
    private RejectedThreadService rejectedThreadService;

    @Inject
    private ClouderaManagerTotalHostsEvaluator totalHostsEvaluatorService;

    @Inject
    private StackResponseUtils stackResponseUtils;

    @Inject
    private CloudbreakCommunicator cloudbreakCommunicator;

    @Override
    public void onApplicationEvent(ScalingEvent event) {
        BaseAlert alert = event.getAlert();
        Cluster cluster = clusterService.findById(alert.getCluster().getId());
        MDCBuilder.buildMdcContext(cluster);

        long remainingTime = ClusterUtils
                .getRemainingCooldownTime(cluster.getCoolDown() * TimeUtil.MIN_IN_MS, cluster.getLastScalingActivity());
        if (remainingTime > 0) {
            handleClusterScaling(cluster, alert.getScalingPolicy());
        } else {
            LOGGER.debug("Cluster cannot be scaled for {} min(s)",
                    ClusterUtils.TIME_FORMAT.format((double) remainingTime / TimeUtil.MIN_IN_MS));
        }
    }

    private void handleClusterScaling(Cluster cluster, ScalingPolicy policy) {
        boolean clusterScaled = false;

        switch (policy.getAdjustmentType()) {
            case LOAD_BASED:
                LoadAlert loadAlert = (LoadAlert) policy.getAlert();
                if (!loadAlert.getDecommissionNodeIds().isEmpty()) {
                    cloudbreakCommunicator.decommissionInstancesForCluster(cluster, loadAlert.getDecommissionNodeIds());
                    clusterScaled = true;
                } else {
                    clusterScaled = scaleBasedOnScalingPolicy(cluster, policy);
                }
                break;
            default:
                clusterScaled = scaleBasedOnScalingPolicy(cluster, policy);
        }

        if (clusterScaled) {
            rejectedThreadService.remove(cluster.getId());
            cluster.setLastScalingActivityCurrent();
            clusterService.save(cluster);
        } else {
            LOGGER.debug("No scaling activity required");
        }
    }

    private boolean scaleBasedOnScalingPolicy(Cluster cluster, ScalingPolicy policy) {
        int hostGroupNodeCount = getHostGroupNodeCount(cluster, policy);
        int desiredNodeCount = getDesiredNodeCount(cluster, policy, hostGroupNodeCount);
        if (hostGroupNodeCount != desiredNodeCount) {
            Runnable scalingRequest = (Runnable) applicationContext.getBean("ScalingRequest", cluster, policy,
                    hostGroupNodeCount, desiredNodeCount);
            executorService.submit(scalingRequest);
            return true;
        }
        return false;
    }

    private int getDesiredNodeCount(Cluster cluster, ScalingPolicy policy, int hostGroupNodeCount) {
        int scalingAdjustment = policy.getScalingAdjustment();
        int desiredHostGroupNodeCount;
        switch (policy.getAdjustmentType()) {
            case NODE_COUNT:
            case LOAD_BASED:
                desiredHostGroupNodeCount = hostGroupNodeCount + scalingAdjustment;
                break;
            case PERCENTAGE:
                desiredHostGroupNodeCount = hostGroupNodeCount
                        + (int) (ceil(hostGroupNodeCount * ((double) scalingAdjustment / ClusterUtils.MAX_CAPACITY)));
                break;
            case EXACT:
                desiredHostGroupNodeCount = policy.getScalingAdjustment();
                break;
            default:
                desiredHostGroupNodeCount = hostGroupNodeCount;
        }
        int minSize = cluster.getHostGroupMinSize();
        int maxSize = cluster.getHostGroupMaxSize();

        return desiredHostGroupNodeCount < minSize ? minSize : desiredHostGroupNodeCount > maxSize ? maxSize : desiredHostGroupNodeCount;
    }

    private int getHostGroupNodeCount(Cluster cluster, ScalingPolicy policy) {
        StackV4Response stackV4Response = cloudbreakCommunicator.getByCrn(cluster.getStackCrn());
        return stackResponseUtils.getNodeCountForHostGroup(stackV4Response, policy.getHostGroup());
    }
}