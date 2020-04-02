package com.sequenceiq.periscope.model;

public class CloudInstanceType {

    private static final Integer GB_TO_MB = 1024;

    private String instanceName;

    private Integer coreCPU;

    private Integer memoryInGB;

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public Integer getCoreCPU() {
        return coreCPU;
    }

    public void setCoreCPU(Integer coreCPU) {
        this.coreCPU = coreCPU;
    }

    public Integer getMemoryInGB() {
        return memoryInGB;
    }

    public void setMemoryInGB(Integer memoryInGB) {
        this.memoryInGB = memoryInGB;
    }

    public Integer getMemoryInMB() {
        return memoryInGB * GB_TO_MB;
    }
}
