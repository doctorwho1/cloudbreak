package com.sequenceiq.periscope.domain;

public class LoadAlertConfiguration {

    private Integer minResourceValue = Cluster.DEFAULT_HOSTGROUP_MIN_SIZE;

    private Integer maxResourceValue = Cluster.DEFAULT_HOSTGROUP_MAX_SIZE;

    private Integer coolDownMinutes = Cluster.DEFAULT_HOSTGROUP_COOL_DOWN;

    public Integer getMinResourceValue() {
        return minResourceValue;
    }

    public void setMinResourceValue(Integer minResourceValue) {
        this.minResourceValue = minResourceValue;
    }

    public Integer getMaxResourceValue() {
        return maxResourceValue;
    }

    public void setMaxResourceValue(Integer maxResourceValue) {
        this.maxResourceValue = maxResourceValue;
    }

    public Integer getCoolDownMinutes() {
        return coolDownMinutes;
    }

    public void setCoolDownMinutes(Integer coolDownMinutes) {
        this.coolDownMinutes = coolDownMinutes;
    }
}
