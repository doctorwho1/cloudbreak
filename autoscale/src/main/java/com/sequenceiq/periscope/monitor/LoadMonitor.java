package com.sequenceiq.periscope.monitor;

import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.api.endpoint.v4.common.StackType;
import com.sequenceiq.periscope.api.model.ClusterState;
import com.sequenceiq.periscope.domain.Cluster;
import com.sequenceiq.periscope.monitor.evaluator.EvaluatorExecutor;
import com.sequenceiq.periscope.monitor.evaluator.load.YarnLoadEvaluator;

@Component
@ConditionalOnProperty(prefix = "periscope.enabledAutoscaleMonitors.load-monitor", name = "enabled", havingValue = "true")
public class LoadMonitor extends ClusterMonitor {

    @Override
    public String getIdentifier() {
        return "load-monitor";
    }

    @Override
    public String getTriggerExpression() {
        return MonitorUpdateRate.EVERY_MIN_RATE_CRON;
    }

    @Override
    public Class<? extends EvaluatorExecutor> getEvaluatorType(Cluster cluster) {
        return getLoadEvaluatorForCluster(cluster);
    }

    @Override
    protected List<Cluster> getMonitored() {
        List<Long> clusterIds = getClusterService().findLoadAlertClustersForNode(StackType.WORKLOAD,
                ClusterState.RUNNING, true, getPeriscopeNodeConfig().getId());
        return clusterIds.isEmpty() ? List.of() : getClusterService().findClustersByClusterIds(clusterIds);
    }

    protected Class<? extends EvaluatorExecutor> getLoadEvaluatorForCluster(Cluster cluster) {
        String policyHostGroup = cluster.getLoadAlerts().stream().findFirst()
                .get().getScalingPolicy().getHostGroup().toLowerCase();

        switch (policyHostGroup) {
            case "compute":
            default:
                return YarnLoadEvaluator.class;
        }
    }
}
