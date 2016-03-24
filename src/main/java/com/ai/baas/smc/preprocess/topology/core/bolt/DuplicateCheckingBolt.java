package com.ai.baas.bmc.topology.core.bolt;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.ai.baas.storm.duplicate.DuplicateCheckingConfig;


public class DuplicateCheckingBolt extends BaseBasicBolt {
	private static final long serialVersionUID = -4549737615575118377L;
	private String[] outputFields;
	
	public DuplicateCheckingBolt(String aOutputFields){
		outputFields = StringUtils.splitPreserveAllTokens(aOutputFields, ",");
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		System.out.println("+++++++++++++++DuplicateCheckingBolt+++++");
		DuplicateCheckingConfig.getInstance();
	}
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String out = input.getString(0);
		System.out.println("-------------------out=="+out);
		System.out.println("-------------------size=="+input.size());
		for(Object obj:input.getValues()){
			System.out.println("*************"+obj.toString());
		}
		collector.emit(new ArrayList<Object>());
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(outputFields));
	}
	

}
