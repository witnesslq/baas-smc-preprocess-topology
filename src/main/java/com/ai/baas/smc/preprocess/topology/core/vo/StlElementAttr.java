package com.ai.baas.smc.preprocess.topology.core.vo;

import java.sql.Timestamp;

public class StlElementAttr {
    private Long attrId;

    private String tenantId;

    private Long elementId;

    private String subObjectId;

    private Long subElementId;

    private String relType;

    private String relValue;

    private String updateDeptId;

    private String updateOperId;

    private Timestamp updateTime;

    public Long getAttrId() {
        return attrId;
    }

    public void setAttrId(Long attrId) {
        this.attrId = attrId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId == null ? null : tenantId.trim();
    }

    public Long getElementId() {
        return elementId;
    }

    public void setElementId(Long elementId) {
        this.elementId = elementId;
    }

    public String getSubObjectId() {
        return subObjectId;
    }

    public void setSubObjectId(String subObjectId) {
        this.subObjectId = subObjectId == null ? null : subObjectId.trim();
    }

    public Long getSubElementId() {
        return subElementId;
    }

    public void setSubElementId(Long subElementId) {
        this.subElementId = subElementId;
    }

    public String getRelType() {
        return relType;
    }

    public void setRelType(String relType) {
        this.relType = relType == null ? null : relType.trim();
    }

    public String getRelValue() {
        return relValue;
    }

    public void setRelValue(String relValue) {
        this.relValue = relValue == null ? null : relValue.trim();
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