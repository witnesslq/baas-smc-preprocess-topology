package com.ai.baas.smc.preprocess.topology.core.bolt;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.ai.baas.dshm.client.CacheFactoryUtil;
import com.ai.baas.dshm.client.impl.CacheBLMapper;
import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.NameSpace;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlBillItemData.ColumnName;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlElement.StatisticsType;
import com.ai.baas.smc.preprocess.topology.core.util.HbaseClient;
import com.ai.baas.smc.preprocess.topology.core.util.SmcSeqUtil;
import com.ai.baas.smc.preprocess.topology.core.vo.StlElementAttr;
import com.ai.baas.smc.preprocess.topology.core.vo.StlElementVo;
import com.ai.baas.storm.exception.BusinessException;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.opt.sdk.constants.ExceptCodeConstants;
import com.ai.opt.sdk.util.StringUtil;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.dubbo.common.json.JSON;

public class StatisticsBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 8475030105476807164L;

    ICacheClient cacheClient = CacheFactoryUtil.getCacheClient(CacheBLMapper.CACHE_BL_CAL_PARAM);

    private MappingRule[] mappingRules = new MappingRule[2];

    private static final Logger logger = LoggerFactory.getLogger(CheckBolt.class);

    private String[] outputFields;

    public StatisticsBolt(String aOutputFields) {
        outputFields = StringUtils.splitPreserveAllTokens(aOutputFields, ",");
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        // TODO Auto-generated method stub
        // TODO Auto-generated method stub
        /* 接收输入报文 */
        String inputData = input.getString(0);
        logger.info("数据校验bolt输入消息报文：[" + inputData + "]...");
        // if (StringUtils.isBlank(inputData)) {
        // logger.error("流水为空");
        // return;
        // }
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
            ICacheClient cacheClient = CacheClientFactory
                    .getCacheClient(NameSpace.OBJECT_POLICY_ELEMENT_CACHE);
            ICacheClient cacheElementAttr = CacheClientFactory
                    .getCacheClient(NameSpace.STL_OBJ_STAT);
            List<Map<String, String>> results = getDataFromDshm(tenantId, batchNo);

            for (Map<String, String> map : results) {
                for (Entry<String, String> result : map.entrySet()) {
                    String objectId = result.getValue();
                    // 根据对象id获取元素ID
                    String elementStrings = cacheClient.get(tenantId + "." + objectId);// key：租户id.流水对象id获得政策id为key元素对象序列为value的map
                    Map mapResult = JSON.parse(elementStrings, Map.class);
                    for (Object o : mapResult.entrySet()) {
                        Map.Entry<Long, String> entry = (Entry<Long, String>) o;
                        Long policyId = entry.getKey();
                        String elements = entry.getValue();
                        List list = JSON.parse(elements, List.class);
                        for (Object oo : list) {
                            StlElementVo stlElementVo = (StlElementVo) oo;
                            // 租户ID+政策ID+账期+统计元素ID+统计元素值@@@@@@@@@@@@@账期不知道在哪取@@@@@@@@@@@@@@@@@
                            assemResult(tenantId, policyId.toString(), "", objectId, stlElementVo
                                    .getElementId().toString(), data.get(stlElementVo
                                    .getElementCode()));
                            // 获得统计元素属性表的对象list组个进行限定条件校验，
                            String elementResult = cacheElementAttr.get(tenantId + "."
                                    + stlElementVo.getElementId().toString());
                            List elementAttrlist = JSON.parse(elementResult, List.class);
                            for (Object object : elementAttrlist) {
                                StlElementAttr stlElementAttr = (StlElementAttr) o;
                            }
                            // 如果满足则根据汇总方式进行累加
                            if (StatisticsType.RECORD_COUNT
                                    .equals(stlElementVo.getStatisticsType())) {

                            } else if (StatisticsType.VALUE_SUM.equals(stlElementVo
                                    .getStatisticsType())) {

                            }
                            // 更新算对象统计数据表的统计值和统计次数（redis:不管限定条件是否满足，统计次数总是累加）。

                        }

                    }

                    // if (list.size() == 0) {
                    // throw new BusinessException(
                    // ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR, tenantId + "."
                    // + objectId + "租户id.流水对象id获得元素对象为空");
                    // }
                    // for (Object o : list) {
                    // StlElementVo stlElementVo = (StlElementVo) o;
                    // String element = data.get(stlElementVo.getElementCode());
                    // }

                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 租户ID+政策ID+账期+统计元素ID+统计元素值
    private void assemResult(String tenantId, String PolicyId, String billTimeSn, String objectId,
            String elementId, String elementValue) throws Exception {
        StringBuilder stlObjStatkey = new StringBuilder();
        stlObjStatkey.append(tenantId);
        stlObjStatkey.append("_");
        stlObjStatkey.append(PolicyId);
        stlObjStatkey.append("_");
        stlObjStatkey.append(billTimeSn);
        stlObjStatkey.append("_");
        stlObjStatkey.append(elementId);
        stlObjStatkey.append("_");
        stlObjStatkey.append(elementValue);
        ICacheClient cacheClient = CacheClientFactory.getCacheClient(NameSpace.STL_OBJ_STAT);
        String result = cacheClient.get(stlObjStatkey.toString());
        if (StringUtil.isBlank(result)) {
            StringBuilder stlObjStatValue = new StringBuilder();
            stlObjStatValue.append(SmcSeqUtil.createDataId());
            stlObjStatValue.append("_");
            stlObjStatValue.append(tenantId);
            stlObjStatValue.append("_");
            stlObjStatValue.append(PolicyId);
            stlObjStatValue.append("_");
            stlObjStatValue.append(billTimeSn);
            stlObjStatValue.append("_");
            stlObjStatValue.append(objectId);
            stlObjStatValue.append("_");
            stlObjStatValue.append(elementId);
            stlObjStatValue.append("_");
            stlObjStatValue.append(elementValue);
            stlObjStatValue.append("_");
            stlObjStatValue.append("0");
            stlObjStatValue.append("_");
            stlObjStatValue.append("0");
            cacheClient.set(stlObjStatkey.toString(), stlObjStatValue.toString());
        }

    }

    private List<Map<String, String>> getDataFromDshm(String tenantId, String batchNo) {
        ICacheClient cacheClient = CacheFactoryUtil
                .getCacheClient(CacheBLMapper.CACHE_BL_CAL_PARAM);
        IDshmClient client = null;
        if (client == null)
            client = new DshmClient();
        Map<String, String> logParam = new TreeMap<String, String>();
        logParam.put(SmcConstants.TENANT_ID_BATCH_NO, tenantId + "." + batchNo);
        return client.list("cp_price_info").where(logParam).executeQuery(cacheClient);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }
}
