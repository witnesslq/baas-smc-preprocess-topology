package com.ai.baas.bmc.topology.core.bolt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;

public class UnpackingBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -8200039989835637219L;
	private static Logger logger = LoggerFactory.getLogger(UnpackingBolt.class);
	private MappingRule[] mappingRules = new MappingRule[2];
	//private String dbName = "jdbc.bmc";
	private String[] outputFields;
	
	
	public UnpackingBolt(String aOutputFields){
		System.out.println("===============UnpackingBolt 00000000000000000====");
		outputFields = StringUtils.splitPreserveAllTokens(aOutputFields, ",");
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		System.out.println("===============UnpackingBolt.prepare====");
		JdbcProxy.loadResource(Arrays.asList(BaseConstants.JDBC_DEFAULT), stormConf);
		mappingRules[0] = MappingRule.getMappingRule(MappingRule.FORMAT_TYPE_INPUT, BaseConstants.JDBC_DEFAULT);
		mappingRules[1] = mappingRules[0];
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = "";
		try{
			line = input.getString(0);
			//System.out.println("=============line=="+line);
			List<Object> values = null;
			String[] inputDatas = StringUtils.splitPreserveAllTokens(line, BaseConstants.RECORD_SPLIT);
			MessageParser messageParser = null;
			for(String inputData:inputDatas){
				//System.out.println("-------------"+inputData);
				messageParser = MessageParser.parseObject(inputData, mappingRules, outputFields);
				messageParser.getData();
				values = messageParser.toTupleData();
				if (CollectionUtils.isNotEmpty(values)){
					collector.emit(values);
				}
				
				//test
//				values = new ArrayList<Object>();
//				values.add(inputData);
//				collector.emit(values);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		System.out.println("===============UnpackingBolt 3333333333333====");
		declarer.declare(new Fields(outputFields));
	}
	

}
