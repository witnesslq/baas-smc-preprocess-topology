package com.ai.baas.smc.preprocess.topology.core.bolt;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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

import com.ai.baas.dshm.client.CacheFactoryUtil;
import com.ai.baas.dshm.client.impl.CacheBLMapper;
import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.DshmTableName;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.NameSpace;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlElement.IsNecessary;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlElement.type;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcExceptCodeConstant;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcHbaseConstants;
import com.ai.baas.smc.preprocess.topology.core.vo.StlElement;
import com.ai.baas.smc.preprocess.topology.core.vo.StlSysParam;
import com.ai.baas.storm.exception.BusinessException;
import com.ai.baas.storm.failbill.FailBillHandler;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.baas.storm.util.HBaseProxy;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.opt.sdk.constants.ExceptCodeConstants;
import com.ai.opt.sdk.util.StringUtil;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.fastjson.JSON;

public class CheckBolt extends BaseBasicBolt {

    private static final long serialVersionUID = -3214008757998306486L;

    private String[] outputFields;

    private MappingRule[] mappingRules = new MappingRule[2];

    private static final Logger logger = LoggerFactory.getLogger(CheckBolt.class);

    private ICacheClient cacheClient;

    private ICacheClient successRecordcacheClient;

    private ICacheClient failedRecordcacheClient;

    private ICacheClient countCacheClient;

    private ICacheClient calParamCacheClient;

    private IDshmClient dshmClient;

    private static Configuration conf;

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
        if (countCacheClient == null) {
            countCacheClient = CacheClientFactory.getCacheClient(NameSpace.CHECK_COUNT_CACHE);
        }
        if (dshmClient == null) {
            dshmClient = new DshmClient();
        }
        if (calParamCacheClient == null) {
            calParamCacheClient = CacheFactoryUtil.getCacheClient(CacheBLMapper.CACHE_BL_CAL_PARAM);
        }
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.1.130.84,10.1.130.85,10.1.236.122");
        conf.set("hbase.zookeeper.property.clientPort", "49181");
        JdbcProxy.loadDefaultResource(stormConf);
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
        // String inputData =
        // "MVNE\u0001msg\u0001msg\u000120160301001\u0001JSMVNE2016030100123hx\u000120160421162542\u0001201604\u0001JSMVNE20160301001\u0001100\u000123\u0001hx\u0001bw\u00011709123478\u000120160323\u0001\"\"\"testcontent\"\"\u0001\"\"testcontent\"\"\u0001\"\"testcontent\"\"\"\u00011";
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
            // String line = input.getStringByField(BaseConstants.RECORD_DATA);
            // logger.info("-------------------line==" + line);
            String tenantId = data.get(BaseConstants.TENANT_ID);
            String batchNo = data.get(SmcConstants.BATCH_NO);
            String orderId = data.get(SmcConstants.ORDER_ID);
            String applyTime = data.get(SmcConstants.APPLY_TIME);
            // 数据导入日志表中查询此批次数据的数据对象(redis)

            // List<Map<String, String>> results = getDataFromDshm(tenantId, batchNo);
            // if (results.size() == 0) {
            // throw new BusinessException(ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
            // tenantId + "." + batchNo + "租户id.批次号在共享内存中获得数据对象为空");
            // }
            // Map<String, String> map = results.get(0);
            String objectId = "msg";
            String billTimeSn = "201603";
            // String objectId = map.get("OBJECT_ID");
            // String billTimeSn = map.get("BILL_TIME_SN");
            // 根据对象id获取元素ID
            String elementStrings = cacheClient.hget(NameSpace.OBJECT_ELEMENT_CACHE, tenantId + "."
                    + objectId);// key：租户id.流水对象id获得元素对象想
            if (StringUtil.isBlank(elementStrings)) {
                throw new BusinessException(ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                        tenantId + "." + objectId + "租户id.流水对象id获得元素对象为空");
            }
            List<StlElement> list = JSON.parseArray(elementStrings, StlElement.class);

            for (StlElement stlElement : list) {
                if (stlElement.getAttrType().equals("normal")) {
                    String element = data.get(stlElement.getElementCode());
                    Boolean NecessaryResult = checkIsNecessary(element, stlElement);
                    if (!NecessaryResult) {
                        // 必填数据为空失败,KEY：租户ID_批次号_数据对象_流水ID_流水产生日期(YYYYMMDD)
                        assemResult(tenantId, batchNo, billTimeSn, objectId, orderId, applyTime,
                                "失败", "必填元素为空");
                        increaseRedise(false, tenantId, batchNo);
                        FailBillHandler.addFailBillMsg(data, SmcConstants.BILL_DETAIL_CHECK_BOLT,
                                SmcExceptCodeConstant.BUSINESS_EXCEPTION, "预处理校验失败");
                        throw new BusinessException(
                                ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                                stlElement.getElementCode() + "校验失败，此elementcode为必填");
                    } else {
                        // @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
                        // Boolean ValueTypeResult = checkValueType(element, stlElement);
                        Boolean ValueTypeResult = true;
                        if (!ValueTypeResult) {
                            assemResult(tenantId, batchNo, billTimeSn, objectId, orderId,
                                    applyTime, "失败", "必填元素为空");
                            increaseRedise(false, tenantId, batchNo);
                            FailBillHandler.addFailBillMsg(data,
                                    SmcConstants.BILL_DETAIL_CHECK_BOLT,
                                    SmcExceptCodeConstant.BUSINESS_EXCEPTION, "预处理校验失败");
                            throw new BusinessException(
                                    ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                                    stlElement.getElementCode() + "校验失败，此elementcode属性值类型错误");
                        } else {
                            Boolean IsPKResult = checkIsPK(tenantId, batchNo, objectId, orderId,
                                    applyTime);
                            // if (!IsPKResult) {
                            // assemResult(tenantId, batchNo, billTimeSn, objectId, orderId,
                            // applyTime, "失败", "是否主键与设定不符");
                            // increaseRedise(successRecordcacheClient, failedRecordcacheClient,
                            // false, tenantId, batchNo);
                            // FailBillHandler.addFailBillMsg(data,
                            // SmcConstants.BILL_DETAIL_CHECK_BOLT,
                            // SmcExceptCodeConstant.BUSINESS_EXCEPTION, "预处理校验失败");
                            // throw new BusinessException(
                            // ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                            // stlElement.getElementCode() + "校验失败，此elementcode是否主键与设定不符");
                            // }
                        }
                    }
                }
            }
            assemResult(tenantId, batchNo, billTimeSn, objectId, orderId, applyTime, "成功", "校验通过");
            increaseRedise(true, tenantId, batchNo);
            collector.emit(new Values(inputData));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void increaseRedise(boolean b, String tenantId, String batchNo) {
        if (b) {
            StringBuilder successBuilder = new StringBuilder();
            successBuilder.append("busiData");
            successBuilder.append("_");
            successBuilder.append(tenantId);
            successBuilder.append("_");
            successBuilder.append(batchNo);
            successBuilder.append("_");
            successBuilder.append("verify_success");
            String successValue = successRecordcacheClient.hget(NameSpace.SUCCESS_RECORD,
                    successBuilder.toString());

            if (StringUtil.isBlank(successValue)) {
                StringBuilder successValueFirst = new StringBuilder();// 业务数据_租户ID _批次号_校验通过记录数
                successValueFirst.append("业务数据");
                successValueFirst.append("_");
                successValueFirst.append(tenantId);
                successValueFirst.append("_");
                successValueFirst.append(batchNo);
                successValueFirst.append("_");
                successValueFirst.append("1");
                successRecordcacheClient.hset(NameSpace.SUCCESS_RECORD, successBuilder.toString(),
                        successValueFirst.toString());
                System.out.println("成功key值为：" + successBuilder.toString() + "value值为："
                        + successValueFirst.toString());
            } else {
                String num = successValue.split("_")[3];
                // @@@@@@@@@@@@@@@@@@@@@@@@@@@@需要问@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
                successRecordcacheClient.incr(successBuilder.toString());
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
            String failedValue = failedRecordcacheClient.hget(NameSpace.FAILED_RECORD,
                    failedBuilder.toString());

            if (StringUtil.isBlank(failedValue)) {
                StringBuilder failedValueValueFirst = new StringBuilder();// 业务数据_租户ID _批次号_校验通过记录数
                failedValueValueFirst.append("业务数据");
                failedValueValueFirst.append("_");
                failedValueValueFirst.append(tenantId);
                failedValueValueFirst.append("_");
                failedValueValueFirst.append(batchNo);
                failedValueValueFirst.append("_");
                failedValueValueFirst.append("1");
                failedRecordcacheClient.hset(NameSpace.FAILED_RECORD, failedBuilder.toString(),
                        failedValueValueFirst.toString());
                System.out.println("失败key值为：" + failedBuilder.toString() + "value值为："
                        + failedValueValueFirst.toString());
            } else {
                String num = failedValue.split("_")[3];
                // Long numLong = countCacheClient.incr(num.getBytes());
                Long numLong = 10l;
                failedValue.replace(num, Long.toString(numLong));
                successRecordcacheClient.hset(NameSpace.SUCCESS_RECORD, failedBuilder.toString(),
                        failedValue);
                System.out
                        .println("失败key值为：" + failedBuilder.toString() + "value值为：" + failedValue);
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
        String tableName = SmcHbaseConstants.TableName.STL_ORDER_DATA + yyyyMm;
        Table tableStlOrderData = HBaseProxy.getConnection().getTable(TableName.valueOf(tableName));
        if (tableStlOrderData == null) {
            throw new BusinessException("1111", "表不存在");
        }
        @SuppressWarnings({ "deprecation", "resource" })
        HBaseAdmin admin = new HBaseAdmin(conf);
        System.out.print(admin.tableExists(tableName));
        if (!admin.tableExists(tableName)) {
            throw new BusinessException("1111", "表不存在");
        }
        Put put = new Put(stlOrderDatakey.toString().getBytes());
        // put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
        // SmcHbaseConstants.StlOrderData.TENANT_ID.getBytes(), tenantId.getBytes());
        // put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
        // SmcHbaseConstants.StlOrderData.BATCH_NO.getBytes(), batchNo.getBytes());
        // put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
        // SmcHbaseConstants.StlOrderData.OBJECT_ID.getBytes(), objectId.getBytes());
        // put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
        // SmcHbaseConstants.StlOrderData.ORDER_ID.getBytes(), orderId.getBytes());
        // put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
        // SmcHbaseConstants.StlOrderData.APPLY_TIME.getBytes(), applyTime.getBytes());
        // put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
        // SmcHbaseConstants.StlOrderData.VERIFY_STATE.getBytes(), verifyState.getBytes());
        // put.addColumn(FamilyColumnName.COLUMN_DEF.getBytes(),
        // SmcHbaseConstants.StlOrderData.VERIFY_DESC.getBytes(), verifydesc.getBytes());
        String testString = "data";
        put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.TENANT_ID.getBytes(),
                tenantId.getBytes());
        put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.BATCH_NO.getBytes(),
                batchNo.getBytes());
        put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.OBJECT_ID.getBytes(),
                objectId.getBytes());
        put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.ORDER_ID.getBytes(),
                orderId.getBytes());
        put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.APPLY_TIME.getBytes(),
                applyTime.getBytes());
        put.addColumn(testString.getBytes(),
                SmcHbaseConstants.StlOrderData.VERIFY_STATE.getBytes(), verifyState.getBytes());
        put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.VERIFY_DESC.getBytes(),
                verifydesc.getBytes());
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
        Table tables = HBaseProxy.getConnection().getTable(TableName.valueOf(tableName));
        Get get = new Get(stlOrderDatakey.toString().getBytes());
        Result result = tables.get(get);
        if (result.isEmpty()) {
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
            String result = cacheClient.hget(NameSpace.OBJECT_ELEMENT_CACHE,
                    stlElement.getTenantId() + "STL_ORDER_DATA" + stlElement.getElementCode());
            if (StringUtil.isBlank(result)) {
                throw new BusinessException(ExceptCodeConstants.Special.PARAM_IS_NULL,
                        stlElement.getTenantId() + stlElement.getElementCode()
                                + "此租户id和元素编码对应的系统参数表数据为空");
            }
            List<StlSysParam> list = JSON.parseArray(result, StlSysParam.class);
            for (StlSysParam stlSysParam : list) {
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
        Map<String, String> params = new TreeMap<String, String>();
        params.put(SmcConstants.DshmKeyName.TENANT_ID, tenantId);
        params.put(SmcConstants.DshmKeyName.BATCH_NO, batchNo);
        return dshmClient.list(DshmTableName.STL_IMPORT_LOG).where(params)
                .executeQuery(calParamCacheClient);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        // declarer.declare(new Fields(outputFields));

    }

}
