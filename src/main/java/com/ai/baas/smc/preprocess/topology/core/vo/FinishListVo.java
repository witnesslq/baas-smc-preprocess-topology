package com.ai.baas.smc.preprocess.topology.core.vo;

public class FinishListVo {
    private String busidata;

    private String tenantId;

    private String batchNo;

    private String billTimeSn;

    private String objectId;

    private String stats_times;

    public String getBusidata() {
        return busidata;
    }

    public void setBusidata(String busidata) {
        this.busidata = busidata;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getBatchNo() {
        return batchNo;
    }

    public void setBatchNo(String batchNo) {
        this.batchNo = batchNo;
    }

    public String getBillTimeSn() {
        return billTimeSn;
    }

    public void setBillTimeSn(String billTimeSn) {
        this.billTimeSn = billTimeSn;
    }

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    public String getStats_times() {
        return stats_times;
    }

    public void setStats_times(String stats_times) {
        this.stats_times = stats_times;
    }

}
