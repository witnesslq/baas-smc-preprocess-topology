package com.ai.baas.smc.preprocess.topology.core.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.expression.ExpressionEvaluator;
import org.wltea.expression.datameta.Variable;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.ai.baas.dshm.client.CacheFactoryUtil;
import com.ai.baas.dshm.client.impl.CacheBLMapper;
import com.ai.baas.dshm.client.impl.DshmClient;
import com.ai.baas.dshm.client.interfaces.IDshmClient;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.DshmTableName;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.NameSpace;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.StlElement.StatisticsType;
import com.ai.baas.smc.preprocess.topology.core.util.IKin;
import com.ai.baas.smc.preprocess.topology.core.util.SmcSeqUtil;
import com.ai.baas.smc.preprocess.topology.core.vo.FinishListVo;
import com.ai.baas.smc.preprocess.topology.core.vo.StlElement;
import com.ai.baas.smc.preprocess.topology.core.vo.StlElementAttr;
import com.ai.baas.storm.exception.BusinessException;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.baas.storm.util.HBaseProxy;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.opt.sdk.constants.ExceptCodeConstants;
import com.ai.opt.sdk.util.StringUtil;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;
import com.alibaba.fastjson.JSON;

public class StatisticsBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 8475030105476807164L;

    private MappingRule[] mappingRules = new MappingRule[2];

    private static final Logger logger = LoggerFactory.getLogger(CheckBolt.class);

    private String[] outputFields;

    private ICacheClient cacheClientObjectToPolicy;

    private ICacheClient cacheClientPolicyToElement;

    private ICacheClient cacheClientElement;

    private ICacheClient cacheClientCount;

    private ICacheClient cacheElementAttr;

    private ICacheClient cacheStatsTimes;

    private ICacheClient countCacheClient;

    private ICacheClient calParamCacheClient;

    private IDshmClient dshmClient;

    private ICacheClient cacheClientStlObjStat;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub
        super.prepare(stormConf, context);

        if (cacheClientObjectToPolicy == null) {
            cacheClientObjectToPolicy = CacheClientFactory
                    .getCacheClient(NameSpace.OBJECT_POLICY_CACHE);
        }
        if (cacheClientPolicyToElement == null) {
            cacheClientPolicyToElement = CacheClientFactory
                    .getCacheClient(NameSpace.POLICY_ELEMENT_CACHE);
        }
        if (cacheClientElement == null) {
            cacheClientElement = CacheClientFactory.getCacheClient(NameSpace.ELEMENT_CACHE);
        }
        if (cacheClientCount == null) {
            cacheClientCount = CacheClientFactory.getCacheClient(NameSpace.STATS_TIMES_COUNT);
        }
        if (cacheElementAttr == null) {
            cacheElementAttr = CacheClientFactory.getCacheClient(NameSpace.STL_ELEMENT_ATTR_CACHE);
        }
        if (cacheStatsTimes == null) {
            cacheStatsTimes = CacheClientFactory.getCacheClient(NameSpace.STATS_TIMES);
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
        if (cacheClientStlObjStat == null) {
            cacheClientStlObjStat = CacheFactoryUtil.getCacheClient(NameSpace.STL_OBJ_STAT);
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

        try {
            String inputData = input.getString(0);
            logger.info("数据校验bolt输入消息报文：[" + inputData + "]...");
            /* 解析报文 */
            MessageParser messageParser = MessageParser.parseObject(inputData, mappingRules,
                    outputFields);
            Map<String, String> data = messageParser.getData();
            String tenantId = data.get(BaseConstants.TENANT_ID);
            String batchNo = data.get(SmcConstants.BATCH_NO);
            int totalRecord = Integer.parseInt(data.get(SmcConstants.TOTAL_RECORD));
            List<Map<String, String>> results = getDataFromDshm(tenantId, batchNo);
            if (results.size() == 0) {
                throw new BusinessException(ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                        tenantId + "." + batchNo + "租户id.批次号在共享内存中获得数据对象为空");
            }
            Map<String, String> map = results.get(0);
            String objectId = map.get("OBJECT_ID");
            String billTimeSn = map.get("BILL_TIME_SN");
            // String objectId = "MSG";
            // String billTimeSn = "201603";
            // 根据对象id获取元素ID
            String tenantIdPolicyStrings = cacheClientObjectToPolicy.hget(
                    NameSpace.OBJECT_POLICY_CACHE, tenantId + "_" + objectId);// key：租户id.流水对象id获得政策id为key元素对象序列为value的map
            if (!StringUtil.isBlank(tenantIdPolicyStrings)) {
                List<String> list = JSON.parseArray(tenantIdPolicyStrings, String.class);
                for (String tenantIdpolicyId : list) {
                    String[] string = tenantIdpolicyId.split("_");
                    String policyIdString = string[1];
                    Long policyId = Long.parseLong(policyIdString);
                    String elements = cacheClientPolicyToElement.hget(
                            NameSpace.POLICY_ELEMENT_CACHE, tenantIdpolicyId);
                    if (!StringUtil.isBlank(elements)) {
                        List<StlElement> elements2 = JSON.parseArray(elements, StlElement.class);
                        for (StlElement stlElement : elements2) {
                            // 租户ID+政策ID+账期+统计元素ID
                            String key = assemKey(tenantId, policyId.toString(), billTimeSn,
                                    stlElement.getElementId().toString());
                            String result = cacheClientStlObjStat.get(key);
                            if (StringUtil.isBlank(result)) {
                                String value = assemValue(tenantId, policyId.toString(),
                                        billTimeSn, objectId, stlElement.getElementId().toString(),
                                        "0", "0");
                                cacheClientStlObjStat.set(key, value);
                            }
                            // 获得统计元素属性表的对象list组个进行限定条件校验，
                            String elementResult = cacheElementAttr.hget(
                                    NameSpace.STL_ELEMENT_ATTR_CACHE, tenantId + "."
                                            + stlElement.getElementId().toString());
                            if (!StringUtil.isBlank(elementResult)) {
                                List<StlElementAttr> elementAttrlist = JSON.parseArray(
                                        elementResult, StlElementAttr.class);
                                boolean flag = true;
                                for (StlElementAttr stlElementAttr : elementAttrlist) {
                                    String matchType = stlElementAttr.getRelType();
                                    String matchValue = stlElementAttr.getRelValue();
                                    String elementValue = data.get(stlElement.getElementCode());
                                    // String elementValue = "12";
                                    if (StringUtil.isBlank(matchType)) {
                                        throw new BusinessException(
                                                ExceptCodeConstants.Special.SYSTEM_ERROR,
                                                stlElementAttr.getAttrId() + "此AttrId对应的RelType为空");
                                    }
                                    if (StringUtil.isBlank(matchValue)) {
                                        throw new BusinessException(
                                                ExceptCodeConstants.Special.SYSTEM_ERROR,
                                                stlElementAttr.getAttrId() + "此AttrId对应的RelValue为空");
                                    }
                                    if (StringUtil.isBlank(elementValue)) {
                                        throw new BusinessException(
                                                ExceptCodeConstants.Special.SYSTEM_ERROR,
                                                stlElement.getElementCode()
                                                        + "此elementCode对应的lementValue为空");
                                    }
                                    if (!checkRel(matchType, matchValue, elementValue)) {
                                        flag = false;
                                        break;
                                    }
                                }
                                // 结算对象统计数据表 统计次数+1
                                String resultValue = cacheClientStlObjStat.get(key);
                                if (flag) { // 如果满足则根据汇总方式进行累加 ,结算对象统计数据表的统计次数加1
                                    if (StatisticsType.RECORD_COUNT.equals(stlElement
                                            .getStatisticsType())) {
                                        increase(resultValue, 1L, key, true);
                                    } else if (StatisticsType.VALUE_SUM.equals(stlElement
                                            .getStatisticsType())) {
                                        Long elementIdString = stlElement.getStatisticsElementId();
                                        // key:tenantId.elementId,value:StlElement
                                        String elementVoString = cacheClientElement.hget(
                                                NameSpace.ELEMENT_CACHE, stlElement.getTenantId()
                                                        + "." + elementIdString);
                                        if (StringUtil.isBlank(elementVoString)) {
                                            throw new BusinessException(
                                                    ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                                                    elementIdString + "此元素id对应的元素为空");
                                        }
                                        StlElement stlElementNew = JSON.parseObject(
                                                elementVoString, StlElement.class);

                                        Long num = Long.parseLong(data.get(stlElementNew
                                                .getElementCode()));
                                        increase(resultValue, num, key, true);
                                    }
                                } else {// 如果不满足则结算对象统计数据表的 统计次数加1
                                    increase(resultValue, 1L, key, false);
                                }
                            }
                        }
                    }
                }
            }
            // key:busidata_租户ID _批次号_stats_times
            // value:业务数据_租户ID _批次号__完成记录数
            String countKey = assemCountKey("busidata", tenantId, batchNo, "stats_times");
            String count = cacheClientCount.hget(NameSpace.STATS_TIMES_COUNT, countKey);
            int num = 0;
            if (StringUtil.isBlank(count)) {
                num = 1;
                cacheClientCount.set(countKey, "1");
            } else {
                num = Integer.parseInt(count) + 1;
                cacheClientCount.set(countKey, Integer.toString(num));
            }
            if (num == totalRecord) {// 加入到缓存的完成队列触发计算拓扑 busidata_租户ID _批次号_账期_数据对象_stats_times

                updateFinishRedis(tenantId, objectId, billTimeSn, batchNo,
                        Integer.toString(totalRecord), cacheStatsTimes);

            } else if (num > totalRecord) {
                throw new BusinessException(ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                        tenantId + "." + batchNo + "此租户id的这个批次统计错误统计数已经超过此批次总数");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateFinishRedis(String tenantId, String objectId, String billTimeSn,
            String batchNo, String totalRecord, ICacheClient cacheStatsTimes) {
        String finishKey = "busidata_tenantId_batchNo_billTimeSn_objectId_stats_times";
        String cacheStatsTimesValues = cacheStatsTimes.get(finishKey);
        if (StringUtil.isBlank(cacheStatsTimesValues)) {
            List<FinishListVo> finishListVos = new ArrayList<FinishListVo>();
            FinishListVo finishListVo = new FinishListVo();
            finishListVo.setBatchNo(batchNo);
            finishListVo.setBillTimeSn(billTimeSn);
            finishListVo.setBusidata("busidata");
            finishListVo.setObjectId(objectId);
            finishListVo.setTenantId(tenantId);
            finishListVo.setStats_times(totalRecord);
            finishListVos.add(finishListVo);
            cacheStatsTimes.set(finishKey, JSON.toJSONString(finishListVos));
        } else {
            List<FinishListVo> list = JSON.parseArray(cacheStatsTimesValues, FinishListVo.class);
            FinishListVo finishListVoNew = new FinishListVo();
            finishListVoNew.setBatchNo(batchNo);
            finishListVoNew.setBillTimeSn(billTimeSn);
            finishListVoNew.setBusidata("busidata");
            finishListVoNew.setObjectId(objectId);
            finishListVoNew.setTenantId(tenantId);
            finishListVoNew.setStats_times(totalRecord);
            list.add(finishListVoNew);
            cacheStatsTimes.set(finishKey, JSON.toJSONString(list));
        }
    }

    private String assemCountKey(String busidata, String tenantId, String batchNo,
            String stats_times) {
        StringBuilder sb = new StringBuilder();
        sb.append(busidata);
        sb.append("_");
        sb.append(tenantId);
        sb.append("_");
        sb.append(batchNo);
        sb.append("_");
        sb.append(stats_times);
        return sb.toString();
    }

    //
    private void increase(String resultRecord, float num, String key, boolean b) {
        String[] result = resultRecord.split("_");
        String statisticsVal = result[6];
        Float times = Float.parseFloat(result[6]);
        Float timesNew = times + 1F;
        StringBuilder resultNew = new StringBuilder();
        resultNew.append(result[0]);
        resultNew.append("_");
        resultNew.append(result[1]);
        resultNew.append("_");
        resultNew.append(result[2]);
        resultNew.append("_");
        resultNew.append(result[3]);
        resultNew.append("_");
        resultNew.append(result[4]);
        resultNew.append("_");
        if (b) {
            resultNew.append(String.valueOf(Float.parseFloat(statisticsVal) + num));
        } else {
            resultNew.append((result[5]));
        }
        resultNew.append("_");
        resultNew.append(timesNew.toString());
        cacheClientStlObjStat.set(key, resultNew.toString());
    }

    private Boolean checkRel(String matchType, String matchValue, String elementValue)
            throws BusinessException {
        Boolean flag = false;
        if (matchType.equals("in")) {
            flag = IKin.in(elementValue, matchValue);
        } else if (matchType.equals("nin")) {
            flag = !IKin.in(elementValue, matchValue);
        } else {
            String expression = "a" + matchType + "b";
            List<Variable> variables = new ArrayList<Variable>();
            variables.add(Variable.createVariable("a", matchValue));
            variables.add(Variable.createVariable("b", elementValue));
            Object resultss = ExpressionEvaluator.evaluate(expression, variables);
            if (resultss == null) {
                throw new BusinessException(ExceptCodeConstants.Special.SYSTEM_ERROR, "a="
                        + matchValue + "符号=" + matchType + "b=" + elementValue
                        + "此形式校验格式不正确,正确格式为a、b为数字，符号为大于小于等");
            }
            flag = Boolean.parseBoolean(resultss.toString());
        }
        return flag;
    }

    // 租户ID+政策ID+账期+统计元素ID

    private String assemKey(String tenantId, String PolicyId, String billTimeSn, String elementId) {
        StringBuilder stlObjStatkey = new StringBuilder();
        stlObjStatkey.append(tenantId);
        stlObjStatkey.append("_");
        stlObjStatkey.append(PolicyId);
        stlObjStatkey.append("_");
        stlObjStatkey.append(billTimeSn);
        stlObjStatkey.append("_");
        stlObjStatkey.append(elementId);
        return stlObjStatkey.toString();
    }

    private String assemValue(String tenantId, String policyId, String billTimeSn, String objectId,
            String elementId, String statisticsVal, String times) {
        StringBuilder stlObjStatValue = new StringBuilder();
        stlObjStatValue.append(tenantId);
        stlObjStatValue.append("_");
        stlObjStatValue.append(policyId);
        stlObjStatValue.append("_");
        stlObjStatValue.append(billTimeSn);
        stlObjStatValue.append("_");
        stlObjStatValue.append(objectId);
        stlObjStatValue.append("_");
        stlObjStatValue.append(elementId);
        stlObjStatValue.append("_");
        stlObjStatValue.append(statisticsVal);
        stlObjStatValue.append("_");
        stlObjStatValue.append(times);
        return stlObjStatValue.toString();
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
