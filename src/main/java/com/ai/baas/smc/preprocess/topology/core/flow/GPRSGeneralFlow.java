package com.ai.baas.bmc.topology.core.flow;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.baas.bmc.topology.core.bolt.DuplicateCheckingBolt;
import com.ai.baas.bmc.topology.core.bolt.UnpackingBolt;
import com.ai.baas.bmc.topology.core.util.BmcConstants;
import com.ai.baas.storm.flow.BaseFlow;
import com.ai.baas.storm.util.BaseConstants;


/**
 * GPRS通用拓扑图
 * @author majun
 * @since 2016.3.16
 */
public class GPRSGeneralFlow extends BaseFlow {
	private static Logger logger = LoggerFactory.getLogger(GPRSGeneralFlow.class);
	
	@Override
	@SuppressWarnings("unchecked")
	public void define() {
		super.setKafkaSpout();
		Map<String,String> outputFieldMapping = (Map<String,String>)conf.get("bmc.gprs.bolt.output.field");
		builder.setBolt(BmcConstants.UNPACKING_BOLT, new UnpackingBolt(outputFieldMapping.get(BmcConstants.UNPACKING_BOLT)), 1).shuffleGrouping(BaseConstants.KAFKA_SPOUT_NAME);
		builder.setBolt("duplicate-checking", new DuplicateCheckingBolt(outputFieldMapping.get(BmcConstants.DUPLICATE_CHECKING_BOLT)), 1).shuffleGrouping(BmcConstants.UNPACKING_BOLT);
		
		
		
	}

	public static void main(String[] args) {
		GPRSGeneralFlow flow = new GPRSGeneralFlow();
		flow.run(args);
	}

	
}
