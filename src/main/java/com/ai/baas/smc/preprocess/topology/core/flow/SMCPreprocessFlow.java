package com.ai.baas.smc.preprocess.topology.core.flow;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.baas.smc.preprocess.topology.core.bolt.CheckBolt;
import com.ai.baas.smc.preprocess.topology.core.bolt.StatisticsBolt;
import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants;
import com.ai.baas.storm.flow.BaseFlow;

/**
 * 结算预处理拓扑图
 * 
 * @author wangjl9
 * @since 2016.4.18
 */
public class SMCPreprocessFlow extends BaseFlow {
    private static Logger logger = LoggerFactory.getLogger(SMCPreprocessFlow.class);

    @Override
    @SuppressWarnings("unchecked")
    public void define() {
        super.setKafkaSpout();
        Map<String, String> outputFieldMapping = (Map<String, String>) conf
                .get("bmc.gprs.bolt.output.field");
        builder.setBolt(SmcConstants.CHECK_BOLT,
                new CheckBolt(outputFieldMapping.get(SmcConstants.CHECK_BOLT)), 1).shuffleGrouping(
                SmcConstants.KAFKA_SPOUT_NAME);
        builder.setBolt("duplicate-checking",
                new StatisticsBolt(outputFieldMapping.get(SmcConstants.STATISTICS_BOLT)), 1)
                .shuffleGrouping(SmcConstants.STATISTICS_BOLT);

    }

    public static void main(String[] args) {
        SMCPreprocessFlow flow = new SMCPreprocessFlow();
        flow.run(args);
    }

}
