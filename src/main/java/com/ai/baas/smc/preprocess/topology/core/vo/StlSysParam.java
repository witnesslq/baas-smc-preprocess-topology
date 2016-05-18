package com.ai.baas.smc.preprocess.topology.core.vo;

import java.sql.Timestamp;

public class StlSysParam {
    private String guidkey;

    private String tenantId;

    private String typeCode;

    private String paramCode;

    private String columnValue;

    private String columnDesc;

    private String subParamCode;

    private String parentValue;

    private Integer dispord;

    private String descb;

    private String state;

    private String updateDeptId;

    private String updateOperId;

    private Timestamp updateTime;

    public String getGuidkey() {
        return guidkey;
    }

    public void setGuidkey(String guidkey) {
        this.guidkey = guidkey == null ? null : guidkey.trim();
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId == null ? null : tenantId.trim();
    }

    public String getTypeCode() {
        return typeCode;
    }

    public void setTypeCode(String typeCode) {
        this.typeCode = typeCode == null ? null : typeCode.trim();
    }

    public String getParamCode() {
        return paramCode;
    }

    public void setParamCode(String paramCode) {
        this.paramCode = paramCode == null ? null : paramCode.trim();
    }

    public String getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue == null ? null : columnValue.trim();
    }

    public String getColumnDesc() {
        return columnDesc;
    }

    public void setColumnDesc(String columnDesc) {
        this.columnDesc = columnDesc == null ? null : columnDesc.trim();
    }

    public String getSubParamCode() {
        return subParamCode;
    }

    public void setSubParamCode(String subParamCode) {
        this.subParamCode = subParamCode == null ? null : subParamCode.trim();
    }

    public String getParentValue() {
        return parentValue;
    }

    public void setParentValue(String parentValue) {
        this.parentValue = parentValue == null ? null : parentValue.trim();
    }

    public Integer getDispord() {
        return dispord;
    }

    public void setDispord(Integer dispord) {
        this.dispord = dispord;
    }

    public String getDescb() {
        return descb;
    }

    public void setDescb(String descb) {
        this.descb = descb == null ? null : descb.trim();
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state == null ? null : state.trim();
    }

    public String getUpdateDeptId() {
        return updateDeptId;
    }

    public void setUpdateDeptId(String updateDeptId) {
        this.updateDeptId = updateDeptId == null ? null : updateDeptId.trim();
    }

    public String getUpdateOperId() {
        return updateOperId;
    }

    public void setUpdateOperId(String updateOperId) {
        this.updateOperId = updateOperId == null ? null : updateOperId.trim();
    }

    public Timestamp getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Timestamp updateTime) {
        this.updateTime = updateTime;
    }
}