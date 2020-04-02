package com.sequenceiq.periscope.service;

import org.springframework.stereotype.Service;

import com.sequenceiq.cloudbreak.auth.altus.Crn;

@Service
public class MachineUserService {

    private static final String AUTOSCALE_DATAHUB_MACHINE_USER_PATTERN = "crn:cdp:iam:us-west-1:altus:user:autoscale";

    public String getAutoscaleMachineUser(String clusterCrn) {
        return String.format(AUTOSCALE_DATAHUB_MACHINE_USER_PATTERN,
                Crn.fromString(clusterCrn));
    }
}
