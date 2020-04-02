package com.sequenceiq.redbeams.flow.redbeams.stop.handler;

import com.sequenceiq.cloudbreak.cloud.CloudConnector;
import com.sequenceiq.cloudbreak.cloud.context.AuthenticatedContext;
import com.sequenceiq.cloudbreak.cloud.context.CloudContext;
import com.sequenceiq.cloudbreak.cloud.init.CloudPlatformConnectors;
import com.sequenceiq.cloudbreak.cloud.scheduler.SyncPollingScheduler;
import com.sequenceiq.cloudbreak.cloud.task.PollTaskFactory;
import com.sequenceiq.cloudbreak.cloud.task.ResourcesStatePollerResult;
import com.sequenceiq.flow.event.EventSelectorUtil;
import com.sequenceiq.flow.reactor.api.handler.EventHandler;
import com.sequenceiq.redbeams.flow.redbeams.common.RedbeamsEvent;
import com.sequenceiq.redbeams.flow.redbeams.stop.event.StopDatabaseServerFailed;
import com.sequenceiq.redbeams.flow.redbeams.stop.event.StopDatabaseServerRequest;
import com.sequenceiq.redbeams.flow.redbeams.stop.event.StopDatabaseServerSuccess;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import reactor.bus.Event;
import reactor.bus.EventBus;

@Component
public class StopDatabaseServerHandler implements EventHandler<StopDatabaseServerRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(StopDatabaseServerHandler.class);

    @Inject
    private CloudPlatformConnectors cloudPlatformConnectors;

    @Inject
    private PollTaskFactory statusCheckFactory;

    @Inject
    private SyncPollingScheduler<ResourcesStatePollerResult> syncPollingScheduler;

    @Inject
    private EventBus eventBus;

    @Override
    public String selector() {
        return EventSelectorUtil.selector(StopDatabaseServerRequest.class);
    }

    @Override
    public void accept(Event<StopDatabaseServerRequest> event) {
        LOGGER.debug("Received event: {}", event);
        StopDatabaseServerRequest request = event.getData();
        CloudContext cloudContext = request.getCloudContext();
        try {
            CloudConnector<Object> connector = cloudPlatformConnectors.get(cloudContext.getPlatformVariant());
            AuthenticatedContext ac = connector.authentication().authenticate(cloudContext, request.getCloudCredential());

            connector.resources().stopDatabaseServer(ac, request.getDbInstanceIdentifier());

            RedbeamsEvent success = new StopDatabaseServerSuccess(request.getResourceId());
            eventBus.notify(success.selector(), new Event<>(event.getHeaders(), success));
            LOGGER.debug("Stopping the database stack successfully finished for {}", cloudContext);
        } catch (Exception e) {
            StopDatabaseServerFailed failure = new StopDatabaseServerFailed(request.getResourceId(), e);
            LOGGER.warn("Error stopping the database stack:", e);
            eventBus.notify(failure.selector(), new Event<>(event.getHeaders(), failure));
        }
    }
}
