package com.ai.baas.smc.preprocess.topology.core.bolt;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.ai.baas.dshm.client.CacheFactoryUtil;
import com.ai.baas.dshm.client.impl.CacheBLMapper;
import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.DshmTableName;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.NameSpace;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlBillItemData.FamilyColumnName;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlElement.IsNecessary;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlElement.IsPrimaryKey;
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
import com.alibaba.dubbo.common.utils.StringUtils;
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

    private ICacheClient sysCacheClient;

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
        if (sysCacheClient == null) {
            sysCacheClient = CacheFactoryUtil.getCacheClient(NameSpace.SYS_PARAM_CACHE);
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
            String tenantId = data.get(BaseConstants.TENANT_ID);
            String batchNo = data.get(SmcConstants.BATCH_NO);

            // String batchNo = data.get(BaseConstants.BATCH_SERIAL_NUMBER);
            String orderId = data.get(SmcConstants.ORDER_ID);
            String applyTime = data.get(SmcConstants.APPLY_TIME);
            // 数据导入日志表中查询此批次数据的数据对象(redis)

            List<Map<String, String>> results = getDataFromDshm(tenantId, batchNo);
            if (results.size() == 0) {
                throw new BusinessException(ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                        tenantId + "." + batchNo + "租户id.批次号在共享内存中获得数据对象为空");
            }
            Map<String, String> map = results.get(0);
            String objectId = map.get("object_id");
            String billTimeSn = map.get("bill_time_sn");
            logger.info("共享内存获得的objectId=" + objectId);
            logger.info("共享内存获得的billTimeSn=" + billTimeSn);

            // 根据对象id获取元素ID
            String elementStrings = cacheClient.hget(NameSpace.OBJECT_ELEMENT_CACHE, tenantId + "."
                    + objectId);// key：租户id.流水对象id获得元素对象想
            if (StringUtil.isBlank(elementStrings)) {
                throw new BusinessException(ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                        tenantId + "." + objectId + "租户id.流水对象id获得元素对象为空");
            }
            List<StlElement> list = JSON.parseArray(elementStrings, StlElement.class);
            logger.info(tenantId + "." + objectId + "租户id.流水对象id获得元素对象为：" + list);
            for (StlElement stlElement : list) {
                if (stlElement.getAttrType().equals("normal")) {
                    String element = data.get(stlElement.getElementCode());
                    System.out.println("元素id" + stlElement.getElementCode() + "元素值" + element);
                    Boolean NecessaryResult = checkIsNecessary(element, stlElement);
                    if (!NecessaryResult) {
                        // 必填数据为空失败,KEY：租户ID_批次号_数据对象_流水ID_流水产生日期(YYYYMMDD)
                        assemResult(tenantId, batchNo, billTimeSn, objectId, orderId, applyTime,
                                "失败", "必填元素为空");
                        increaseRedise(false, tenantId, batchNo);
                        // @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
                        // collector.emit(new Values(inputData));
                        FailBillHandler.addFailBillMsg(data, SmcConstants.BILL_DETAIL_CHECK_BOLT,
                                SmcExceptCodeConstant.BUSINESS_EXCEPTION, "预处理校验失败");
                        throw new BusinessException(
                                ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                                stlElement.getElementCode() + "校验失败，此elementcode为必填");
                    } else {
                        Boolean ValueTypeResult = checkValueType(element, stlElement);
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
                            Boolean IsPKResult = checkIsPK(stlElement, tenantId, batchNo, objectId,
                                    orderId, applyTime);
                            if (!IsPKResult) {
                                assemResult(tenantId, batchNo, billTimeSn, objectId, orderId,
                                        applyTime, "失败", "是否主键与设定不符");
                                increaseRedise(false, tenantId, batchNo);
                                FailBillHandler.addFailBillMsg(data,
                                        SmcConstants.BILL_DETAIL_CHECK_BOLT,
                                        SmcExceptCodeConstant.BUSINESS_EXCEPTION, "预处理校验失败");
                                throw new BusinessException(
                                        ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                                        stlElement.getElementCode() + "校验失败，此elementcode是否主键与设定不符");
                            }
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
            long num = countCacheClient.incr(successBuilder.toString());
            logger.info("成功记录数加1后为" + num);
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
            long numFailed = countCacheClient.incr(failedBuilder.toString());
            logger.info("成功记录数加1后为" + numFailed);
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
        @SuppressWarnings("deprecation")
        HBaseAdmin admin = new HBaseAdmin(conf);
        System.out.print(admin.tableExists(tableName));
        if (!admin.tableExists(tableName)) {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.addFamily(new HColumnDescriptor(FamilyColumnName.COLUMN_DEF.getBytes()));
            admin.createTable(tableDesc);
            System.out.println("新建的hbase表名为：" + tableName);

        }
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
        // String testString = "data";
        // put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.TENANT_ID.getBytes(),
        // tenantId.getBytes());
        // put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.BATCH_NO.getBytes(),
        // batchNo.getBytes());
        // put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.OBJECT_ID.getBytes(),
        // objectId.getBytes());
        // put.addColumn(testString.getBytes(), SmcHbaseConstants.StlOrderData.ORDER_ID.getBytes(),
        // orderId.getBytes());
        // put.addColumn(testString.getBytes(),
        // SmcHbaseConstants.StlOrderData.APPLY_TIME.getBytes(),
        // applyTime.getBytes());
        // put.addColumn(testString.getBytes(),
        // SmcHbaseConstants.StlOrderData.VERIFY_STATE.getBytes(), verifyState.getBytes());
        // put.addColumn(testString.getBytes(),
        // SmcHbaseConstants.StlOrderData.VERIFY_DESC.getBytes(),
        // verifydesc.getBytes());
        tableStlOrderData.put(put);
    }

    private Boolean checkIsPK(StlElement stlElement, String tenantId, String batchNo,
            String objectId, String orderId, String applyTime) throws IOException {
        boolean boolresult = true;
        int a = 0;
        if (stlElement.getIsPrimaryKey().equals(IsPrimaryKey.YES)) {
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
                boolresult = true;

            } else {
                boolresult = false;
            }
        }
        return boolresult;
    }

    private Boolean checkValueType(String element, StlElement stlElement) throws Exception {
        String valueType = stlElement.getValueType();
        if (type.ENUM.equals(valueType)) {
            Boolean flag = false; // 系统参数表中获得类型
            String result = sysCacheClient.get(stlElement.getTenantId() + "." + "STL_ORDER_DATA"
                    + "." + stlElement.getElementCode());
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
                throw new BusinessException(e.getMessage(), "int型转换失败");
            }
        } else if (type.FLOAT.equals(valueType)) {
            try {
                Float.parseFloat(element);
            } catch (Exception e) {
                throw new BusinessException(e.getMessage(), "Float型转换失败");
            }

        } else if (type.DATETIME.equals(valueType)) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("YYYYMMDDhhmmss");
                Date date = new Date();
                date = sdf.parse(element); // Mon Jan 14 00:00:00 CST 2013

            } catch (Exception e) {
                throw new BusinessException(e.getMessage(), element + "日期yyyyMMddhhmmss型转换失败");
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
        declarer.declare(new Fields("DATA"));

    }

}
