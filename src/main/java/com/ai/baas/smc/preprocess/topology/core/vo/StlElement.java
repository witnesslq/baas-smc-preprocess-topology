package com.ai.baas.smc.preprocess.topology.core.vo;

import java.sql.Timestamp;

public class StlElement {
    private Long elementId;

    private String tenantId;

    private String elementCode;

    private String elementName;

    private String objectId;

    private String attrType;

    private String isSettlement;

    private String valueType;

    private String isNecessary;

    private String elementType;

    private String dataCreateType;

    private String isPrimaryKey;

    private String unit;

    private Integer sortId;

    private String elementDesc;

    private String statisticsType;

    private String statisticsCyc;

    private String statisticsObjectId;

    private Long statisticsElementId;

    private String state;

    private String createDeptId;

    private String createOperId;

    private Timestamp createTime;

    private String updateDeptId;

    private String updateOperId;

    private Timestamp updateTime;

    public Long getElementId() {
        return elementId;
    }

    public void setElementId(Long elementId) {
        this.elementId = elementId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId == null ? null : tenantId.trim();
    }

    public String getElementCode() {
        return elementCode;
    }

    public void setElementCode(String elementCode) {
        this.elementCode = elementCode == null ? null : elementCode.trim();
    }

    public String getElementName() {
        return elementName;
    }

    public void setElementName(String elementName) {
        this.elementName = elementName == null ? null : elementName.trim();
    }

    public String getObjectId() {
        return objectId;
    }

    public void setObjectId(String objectId) {
        this.objectId = objectId == null ? null : objectId.trim();
    }

    public String getAttrType() {
        return attrType;
    }

    public void setAttrType(String attrType) {
        this.attrType = attrType == null ? null : attrType.trim();
    }

    public String getIsSettlement() {
        return isSettlement;
    }

    public void setIsSettlement(String isSettlement) {
        this.isSettlement = isSettlement == null ? null : isSettlement.trim();
    }

    public String getValueType() {
        return valueType;
    }

    public void setValueType(String valueType) {
        this.valueType = valueType == null ? null : valueType.trim();
    }

    public String getIsNecessary() {
        return isNecessary;
    }

    public void setIsNecessary(String isNecessary) {
        this.isNecessary = isNecessary == null ? null : isNecessary.trim();
    }

    public String getElementType() {
        return elementType;
    }

    public void setElementType(String elementType) {
        this.elementType = elementType == null ? null : elementType.trim();
    }

    public String getDataCreateType() {
        return dataCreateType;
    }

    public void setDataCreateType(String dataCreateType) {
        this.dataCreateType = dataCreateType == null ? null : dataCreateType.trim();
    }

    public String getIsPrimaryKey() {
        return isPrimaryKey;
    }

    public void setIsPrimaryKey(String isPrimaryKey) {
        this.isPrimaryKey = isPrimaryKey == null ? null : isPrimaryKey.trim();
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit == null ? null : unit.trim();
    }

    public Integer getSortId() {
        return sortId;
    }

    public void setSortId(Integer sortId) {
        this.sortId = sortId;
    }

    public String getElementDesc() {
        return elementDesc;
    }

    public void setElementDesc(String elementDesc) {
        this.elementDesc = elementDesc == null ? null : elementDesc.trim();
    }

    public String getStatisticsType() {
        return statisticsType;
    }

    public void setStatisticsType(String statisticsType) {
        this.statisticsType = statisticsType == null ? null : statisticsType.trim();
    }

    public String getStatisticsCyc() {
        return statisticsCyc;
    }

    public void setStatisticsCyc(String statisticsCyc) {
        this.statisticsCyc = statisticsCyc == null ? null : statisticsCyc.trim();
    }

    public String getStatisticsObjectId() {
        return statisticsObjectId;
    }

    public void setStatisticsObjectId(String statisticsObjectId) {
        this.statisticsObjectId = statisticsObjectId == null ? null : statisticsObjectId.trim();
    }

    public Long getStatisticsElementId() {
        return statisticsElementId;
    }

    public void setStatisticsElementId(Long statisticsElementId) {
        this.statisticsElementId = statisticsElementId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state == null ? null : state.trim();
    }

    public String getCreateDeptId() {
        return createDeptId;
    }

    public void setCreateDeptId(String createDeptId) {
        this.createDeptId = createDeptId == null ? null : createDeptId.trim();
    }

    public String getCreateOperId() {
        return createOperId;
    }

    public void setCreateOperId(String createOperId) {
        this.createOperId = createOperId == null ? null : createOperId.trim();
    }

    public Timestamp getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Timestamp createTime) {
        this.createTime = createTime;
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