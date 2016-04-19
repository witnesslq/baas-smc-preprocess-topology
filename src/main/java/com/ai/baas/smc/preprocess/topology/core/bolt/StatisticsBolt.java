package com.ai.baas.smc.preprocess.topology.core.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.expression.ExpressionEvaluator;
import org.wltea.expression.datameta.Variable;

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

    private ICacheClient cacheClient;

    private ICacheClient cacheClientElement;

    private ICacheClient cacheClientCount;

    private ICacheClient cacheElementAttr;

    private ICacheClient cacheStatsTimes;

    private ICacheClient countCacheClient;

    private ICacheClient calParamCacheClient;

    private IDshmClient dshmClient;

    // public StatisticsBolt(String aOutputFields) {
    // outputFields = StringUtils.splitPreserveAllTokens(aOutputFields, ",");
    // }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub
        super.prepare(stormConf, context);
        if (cacheClient == null) {
            cacheClient = CacheClientFactory.getCacheClient(NameSpace.OBJECT_ELEMENT_CACHE);
        }
        if (cacheClientElement == null) {
            cacheClientElement = CacheClientFactory.getCacheClient(NameSpace.ELEMENT_CACHE);
        }
        if (cacheClientCount == null) {
            cacheClientCount = CacheClientFactory.getCacheClient(NameSpace.STATS_TIMES_COUNT);
        }
        if (cacheElementAttr == null) {
            cacheElementAttr = CacheClientFactory.getCacheClient(NameSpace.STL_OBJ_STAT);
        }
        if (cacheStatsTimes == null) {
            cacheStatsTimes = CacheClientFactory.getCacheClient(NameSpace.STATS_TIMES);
        }
        if (countCacheClient == null) {
            countCacheClient = CacheClientFactory.getCacheClient(NameSpace.STATS_TIMES);
        }
        if (dshmClient == null) {
            dshmClient = new DshmClient();
        }
        if (calParamCacheClient == null) {
            calParamCacheClient = CacheFactoryUtil.getCacheClient(CacheBLMapper.CACHE_BL_CAL_PARAM);
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
            String line = input.getStringByField(BaseConstants.RECORD_DATA);
            logger.info("-------------------line==" + line);
            String tenantId = data.get(BaseConstants.TENANT_ID);
            String batchNo = data.get(SmcConstants.BATCH_NO);
            int totalRecord = Integer.parseInt(data.get(SmcConstants.TOTAL_RECORD));
            List<Map<String, String>> results = getDataFromDshm(tenantId, batchNo);
            Map<String, String> map = results.get(0);
            String objectId = map.get("OBJECT_ID");
            String billTimeSn = map.get("BILL_TIME_SN");
            // 根据对象id获取元素ID
            String elementStrings = cacheClient.get(tenantId + "." + objectId);// key：租户id.流水对象id获得政策id为key元素对象序列为value的map
            if (StringUtil.isBlank(elementStrings)) {
                Map mapResult = JSON.parseObject(elementStrings, Map.class);
                for (Object o : mapResult.entrySet()) {
                    Map.Entry<Long, String> entry = (Entry<Long, String>) o;
                    Long policyId = entry.getKey();
                    String elements = entry.getValue();
                    List list = JSON.parseObject(elements, List.class);
                    if (list.size() != 0) {
                        for (Object oo : list) {

                            StlElement stlElementVo = (StlElement) oo;
                            // 租户ID+政策ID+账期+统计元素ID
                            String key = assemKey(tenantId, policyId.toString(), billTimeSn,
                                    stlElementVo.getElementId().toString());
                            ICacheClient cacheClientStlObjStat = CacheClientFactory
                                    .getCacheClient(NameSpace.STL_OBJ_STAT);
                            String result = cacheClientStlObjStat.get(key);
                            if (StringUtil.isBlank(result)) {
                                String value = assemValue(tenantId, policyId.toString(),
                                        billTimeSn, objectId, stlElementVo.getElementId()
                                                .toString(), "0", "0");
                                cacheClientStlObjStat.set(key, value);
                            }
                            // 获得统计元素属性表的对象list组个进行限定条件校验，
                            String elementResult = cacheElementAttr.get(tenantId + "."
                                    + stlElementVo.getElementId().toString());
                            List elementAttrlist = JSON.parseObject(elementResult, List.class);
                            if (elementAttrlist.size() != 0) {
                                boolean flag = true;
                                for (Object object : elementAttrlist) {
                                    StlElementAttr stlElementAttr = (StlElementAttr) o;
                                    String matchType = stlElementAttr.getRelType();
                                    String matchValue = stlElementAttr.getRelValue();
                                    String elementValue = data.get(stlElementVo.getElementCode());
                                    if (!checkRel(matchType, matchValue, elementValue)) {
                                        flag = false;
                                        break;
                                    }
                                }
                                // 结算对象统计数据表 统计次数+1
                                String resultValue = cacheClientStlObjStat.get(key);
                                if (flag) { // 如果满足则根据汇总方式进行累加 ,结算对象统计数据表的统计次数加1
                                    if (StatisticsType.RECORD_COUNT.equals(stlElementVo
                                            .getStatisticsType())) {

                                        increase(resultValue, 1L, cacheClientStlObjStat, key, true);

                                    } else if (StatisticsType.VALUE_SUM.equals(stlElementVo
                                            .getStatisticsType())) {
                                        Long elementIdString = stlElementVo
                                                .getStatisticsElementId();
                                        // key:tenantId.elementId,value:StlElement
                                        String elementVoString = cacheClientElement
                                                .get(stlElementVo.getTenantId() + "."
                                                        + elementIdString);
                                        if (StringUtil.isBlank(elementVoString)) {
                                            throw new BusinessException(
                                                    ExceptCodeConstants.Special.NO_DATA_OR_CACAE_ERROR,
                                                    elementIdString + "此元素id对应的元素为空");
                                        }
                                        StlElement stlElement = JSON.parseObject(elementVoString,
                                                StlElement.class);

                                        Long num = Long.parseLong(data.get(stlElement
                                                .getElementCode()));
                                        increase(resultValue, num, cacheClientStlObjStat, key, true);
                                    }
                                } else {// 如果不满足则结算对象统计数据表的 统计次数加1
                                    increase(resultValue, 1L, cacheClientStlObjStat, key, false);
                                }
                            }
                        }
                    }
                }
            }
            // key:busidata_租户ID _批次号_stats_times
            // value:业务数据_租户ID _批次号__完成记录数
            String countKey = assemCountKey("busidata", tenantId, batchNo, "stats_times");
            String count = cacheClientCount.get(countKey);
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
            List<FinishListVo> list = JSON.parseObject(cacheStatsTimesValues, List.class);
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
    private void increase(String resultRecord, Long num, ICacheClient cacheClientStlObjStat,
            String key, boolean b) {
        String[] result = resultRecord.split("_");
        String statisticsVal = result[6];
        String times = result[7];
        Long timesNew = Long.parseLong(times) + 1l;
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
        resultNew.append(result[5]);
        resultNew.append("_");
        if (b) {
            Long statisticsValNew = countCacheClient.incrBy(statisticsVal.getBytes(), num);
            resultNew.append(statisticsValNew.toString());
        } else {
            resultNew.append((result[6]));
        }
        resultNew.append("_");
        resultNew.append(timesNew.toString());
        cacheClientStlObjStat.set(key, resultNew.toString());
    }

    private Boolean checkRel(String matchType, String matchValue, String elementValue) {
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
        stlObjStatValue.append(SmcSeqUtil.createDataId());
        stlObjStatValue.append("_");
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
        Map<String, String> logParam = new TreeMap<String, String>();
        logParam.put(SmcConstants.BATCH_NO_TENANT_ID, batchNo + ":" + tenantId);
        return dshmClient.list("cp_price_info").where(logParam).executeQuery(calParamCacheClient);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub

    }
}
