package com.ai.baas.smc.preprocess.topology.core.bolt;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.NameSpace;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlBillItemData.FamilyColumnName;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlElement.IsNecessary;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlElement.type;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcHbaseConstants;
import com.ai.baas.smc.preprocess.topology.core.util.HbaseClient;
import com.ai.baas.smc.preprocess.topology.core.vo.StlElement;
import com.ai.baas.smc.preprocess.topology.core.vo.StlSysParam;
import com.ai.baas.storm.exception.BusinessException;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.baas.storm.util.HBaseProxy;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.opt.sdk.constants.ExceptCodeConstants;
import com.ai.opt.sdk.util.StringUtil;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.dubbo.common.json.JSON;

public class CheckBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -3214008757998306486L;

    private String[] outputFields;

    private MappingRule[] mappingRules = new MappingRule[2];

    private static final Logger logger = LoggerFactory.getLogger(CheckBolt.class);

    private ICacheClient cacheClient;

    private ICacheClient successRecordcacheClient;

    private ICacheClient failedRecordcacheClient;

    // public CheckBolt(String aOutputFields) {
    // outputFields = StringUtils.splitPreserveAllTokens(aOutputFields, ",");
    // }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub
        super.prepare(stormConf, context);
        if (cacheClient == null) {
            cacheClient = CacheClientFactory.getCacheClient(NameSpace.OBJECT_ELEMENT_CACHE);
        }
        if (successRecordcacheClient == null) {
            successRecordcacheClient = CacheClientFactory.getCacheClient(NameSpace.SUCCESS_RECORD);
        }
        if (failedRecordcacheClient == null) {
            failedRecordcacheClient = CacheClientFactory.getCacheClient(NameSpace.FAILED_RECORD);
        }

        /* 初始化hbase */
        HBaseProxy.loadResource(stormConf);
        /* 2.获取报文格式信息 */
        mappingRules[0] = MappingRule.getMappingRule(MappingRule.FORMAT_TYPE_OUTPUT,
                BaseConstants.JDBC_DEFAULT);
        mappingRules[1] = mappingRules[0];
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // TODO Auto-generated method stub
        /* 接收输入报文 */
        String inputData = input.getString(0);
        logger.info("数据校验bolt输入消息报文：[" + inputData + "]...");
        if (StringUtils.isBlank(inputData)) {
            logger.error("流水为空");
            return;
        }
        /* 解析报文 */
        MessageParser messageParser = null;
        try {
            messageParser = MessageParser.parseObject(inputData, mappingRules, outputFields);

            Map<String, String> data = messageParser.getData();
            String line = input.getStringByField(BaseConstants.RECORD_DATA);
            logger.info("-------------------line==" + line);
            String tenantId = data.get(BaseConstants.TENANT_ID);
            String batchNo = data.get(SmcConstants.BATCH_NO);
            String orderId = data.get(SmcConstants.ORDER_ID);
            String applyTime = data.get(SmcConstants.APPLY_TIME);
            // 数据导入日志表中查询此批次数据的数据对象(redis)

            List<Map<String, String>> results = getDataFromDshm(tenantId, batchNo);

            Map<String, String> map = results.get(0);
            String objectId = map.get("OBJECT_ID");
            String billTimeSn = map.get("BILL_TIME_SN");
            // 根据对象id获取元素ID
            String elementStrings = cacheClient.get(tenantId + "." + objectId);// key：租户id.流水对象id获得元素对象想
            List list = JSON.parse(elementStrings, List.class);

            if (list.size() == 0) {
                throw new BusinessException(ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                        tenantId + "." + objectId + "租户id.流水对象id获得元素对象为空");
            }
            for (Object o : list) {
                StlElement stlElement = (StlElement) o;
                if (stlElement.getAttrType().equals("normal")) {
                    String element = data.get(stlElement.getElementCode());
                    Boolean NecessaryResult = checkIsNecessary(element, stlElement);
                    if (!NecessaryResult) {
                        // 必填数据为空失败,KEY：租户ID_批次号_数据对象_流水ID_流水产生日期(YYYYMMDD)
                        assemResult(tenantId, batchNo, billTimeSn, objectId, orderId, applyTime,
                                "失败", "必填元素为空");
                        increaseRedise(successRecordcacheClient, failedRecordcacheClient, false,
                                tenantId, batchNo);
                        throw new BusinessException(
                                ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                                stlElement.getElementCode() + "校验失败，此elementcode为必填");

                    } else {
                        Boolean ValueTypeResult = checkValueType(element, stlElement);
                        if (!ValueTypeResult) {
                            assemResult(tenantId, batchNo, billTimeSn, objectId, orderId,
                                    applyTime, "失败", "必填元素为空");
                            increaseRedise(successRecordcacheClient, failedRecordcacheClient,
                                    false, tenantId, batchNo);
                            throw new BusinessException(
                                    ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                                    stlElement.getElementCode() + "校验失败，此elementcode属性值类型错误");
                        } else {
                            Boolean IsPKResult = checkIsPK(tenantId, batchNo, objectId, orderId,
                                    applyTime);
                            if (!IsPKResult) {
                                assemResult(tenantId, batchNo, billTimeSn, objectId, orderId,
                                        applyTime, "失败", "是否主键与设定不符");
                                increaseRedise(successRecordcacheClient, failedRecordcacheClient,
                                        false, tenantId, batchNo);
                                throw new BusinessException(
                                        ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                                        stlElement.getElementCode() + "校验失败，此elementcode是否主键与设定不符");
                            } else {
                                assemResult(tenantId, batchNo, billTimeSn, objectId, orderId,
                                        applyTime, "成功", "校验通过");
                                increaseRedise(successRecordcacheClient, failedRecordcacheClient,
                                        true, tenantId, batchNo);
                                collector.emit(new Values(inputData));

                            }
                        }
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void increaseRedise(ICacheClient successRecordcacheClient,
            ICacheClient failedRecordcacheClient, boolean b, String tenantId, String batchNo) {
        if (b) {
            StringBuilder successBuilder = new StringBuilder();
            successBuilder.append("busiData");
            successBuilder.append("_");
            successBuilder.append(tenantId);
            successBuilder.append("_");
            successBuilder.append(batchNo);
            successBuilder.append("_");
            successBuilder.append("verify_success");
            String successValue = successRecordcacheClient.get(successBuilder.toString());

            if (StringUtil.isBlank(successValue)) {
                StringBuilder successValueFirst = new StringBuilder();// 业务数据_租户ID _批次号_校验通过记录数
                successValueFirst.append("业务数据");
                successValueFirst.append("_");
                successValueFirst.append(tenantId);
                successValueFirst.append("_");
                successValueFirst.append(batchNo);
                successValueFirst.append("_");
                successValueFirst.append("1");
                successRecordcacheClient.set(successBuilder.toString(),
                        successValueFirst.toString());
            } else {
                String num = successValue.split("-")[3];
                int numInt = Integer.parseInt(num) + 1;
                successValue.replace(num, Integer.toString(numInt));
                successRecordcacheClient.set(successBuilder.toString(), successValue);
            }
            // 成功记录数加1
        } else {

            StringBuilder failedBuilder = new StringBuilder();
            failedBuilder.append("busiData");
            failedBuilder.append("_");
            failedBuilder.append(tenantId);
            failedBuilder.append("_");
            failedBuilder.append(batchNo);
            failedBuilder.append("_");
            failedBuilder.append("verify_failure");
            String failedValue = failedRecordcacheClient.get(failedBuilder.toString());

            if (StringUtil.isBlank(failedValue)) {
                StringBuilder failedValueValueFirst = new StringBuilder();// 业务数据_租户ID _批次号_校验通过记录数
                failedValueValueFirst.append("业务数据");
                failedValueValueFirst.append("_");
                failedValueValueFirst.append(tenantId);
                failedValueValueFirst.append("_");
                failedValueValueFirst.append(batchNo);
                failedValueValueFirst.append("_");
                failedValueValueFirst.append("1");
                failedRecordcacheClient.set(failedBuilder.toString(),
                        failedValueValueFirst.toString());
            } else {
                String num = failedValue.split("-")[3];
                int numInt = Integer.parseInt(num) + 1;
                failedValue.replace(num, Integer.toString(numInt));
                successRecordcacheClient.set(failedBuilder.toString(), failedValue);
            }
            // 失败记录数加1
        }
    }

    private void assemResult(String tenantId, String batchNo, String billTimeSn, String objectId,
            String orderId, String applyTime, String verifyState, String verifydesc)
            throws Exception {
        String yyyyMm = billTimeSn.substring(0, 6);
        StringBuilder stlOrderDatakey = new StringBuilder();
        stlOrderDatakey.append(tenantId);
        stlOrderDatakey.append("_");
        stlOrderDatakey.append(batchNo);
        stlOrderDatakey.append("_");
        stlOrderDatakey.append(objectId);
        stlOrderDatakey.append("_");
        stlOrderDatakey.append(orderId);
        stlOrderDatakey.append("_");
        stlOrderDatakey.append(applyTime);

        Table tableStlOrderData = HBaseProxy.getConnection().getTable(
                TableName.valueOf(SmcHbaseConstants.TableName.STL_ORDER_DATA + yyyyMm));
        Put put = new Put(stlOrderDatakey.toString().getBytes());
        put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
                SmcHbaseConstants.StlOrderData.TENANT_ID.getBytes(), tenantId.getBytes());
        put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
                SmcHbaseConstants.StlOrderData.BATCH_NO.getBytes(), batchNo.getBytes());
        put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
                SmcHbaseConstants.StlOrderData.OBJECT_ID.getBytes(), objectId.getBytes());
        put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
                SmcHbaseConstants.StlOrderData.ORDER_ID.getBytes(), orderId.getBytes());
        put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
                SmcHbaseConstants.StlOrderData.APPLY_TIME.getBytes(), applyTime.getBytes());
        put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
                SmcHbaseConstants.StlOrderData.VERIFY_STATE.getBytes(), verifyState.getBytes());
        put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
                SmcHbaseConstants.StlOrderData.VERIFY_DESC.getBytes(), verifydesc.getBytes());
        tableStlOrderData.put(put);
    }

    private Boolean checkIsPK(String tenantId, String batchNo, String objectId, String orderId,
            String applyTime) throws IOException {
        StringBuilder stlOrderDatakey = new StringBuilder();
        stlOrderDatakey.append(tenantId);
        stlOrderDatakey.append("_");
        stlOrderDatakey.append(batchNo);
        stlOrderDatakey.append("_");
        stlOrderDatakey.append(objectId);
        stlOrderDatakey.append("_");
        stlOrderDatakey.append(orderId);
        stlOrderDatakey.append("_");
        stlOrderDatakey.append(applyTime);
        String tableName = "stl_order_data_" + applyTime.substring(0, 6);
        Result result = HbaseClient.getResult(tableName, stlOrderDatakey.toString());
        if (result.list().size() == 0) {
            return true;

        } else {
            return false;
        }
    }

    private Boolean checkValueType(String element, StlElement stlElement) throws Exception {
        String valueType = stlElement.getValueType();
        if (type.ENUM.equals(valueType)) {
            Boolean flag = false;
            // 系统参数表中获得类型
            CacheClientFactory.getCacheClient(NameSpace.SYS_PARAM_CACHE);
            String result = cacheClient.get(stlElement.getTenantId() + "STL_ORDER_DATA"
                    + stlElement.getElementCode());
            List list = JSON.parse(result, List.class);
            for (Object o : list) {
                StlSysParam stlSysParam = (StlSysParam) o;
                if (element.equals(stlSysParam.getColumnValue())) {
                    flag = true;
                    break;
                }
            }
            if (flag) {
                return true;
            } else {
                return false;
            }
        } else if (type.INT.equals(valueType)) {
            try {
                Integer.parseInt(element);
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        } else if (type.FLOAT.equals(valueType)) {

            Float.parseFloat(element);
        } else if (type.DATETIME.equals(valueType)) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmiss");
                Date date = new Date();
                date = sdf.parse(element); // Mon Jan 14 00:00:00 CST 2013

            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    private Boolean checkIsNecessary(String element, StlElement stlElement)
            throws BusinessException {
        Boolean result = true;
        // 根据元素编码获得流水中元素的值
        if (IsNecessary.YES.equals(stlElement.getIsNecessary()) && StringUtil.isBlank(element)) {
            result = false;

        }
        return result;
    }

    private List<Map<String, String>> getDataFromDshm(String tenantId, String batchNo) {
        IDshmClient client = null;
        if (client == null)
            client = new DshmClient();
        Map<String, String> logParam = new TreeMap<String, String>();
        logParam.put(SmcConstants.BATCH_NO_TENANT_ID, batchNo + ":" + tenantId);
        return client.list("cp_price_info").where(logParam).executeQuery(cacheClient);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        // declarer.declare(new Fields(outputFields));

    }

}
