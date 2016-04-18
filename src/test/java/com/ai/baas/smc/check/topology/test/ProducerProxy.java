package com.ai.baas.smc.check.topology.test;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerProxy {
	private static String kafka_name = "smccheckkafka";
	private static Producer<String, String> producer = null;
	private static ProducerProxy instance = null;
	
	public static ProducerProxy getInstance(){
		if(instance == null){
			synchronized(ProducerProxy.class){
				if(instance == null){
					instance = new ProducerProxy();
					init();
				}
			}
		}
		return instance;
	}
	
	private static void init(){
		Properties props = new Properties();
		props.put("metadata.broker.list", "10.1.130.84:39091");
		//props.put("metadata.broker.list", "10.1.241.34:39091");
        props.put("serializer.class", "kafka.serializer.StringEncoder"); 
        props.put("request.required.acks", "1");
        producer = new Producer<String, String>(new ProducerConfig(props));
	}
	
	public void sendMessage(String message){
		KeyedMessage<String, String> data = new KeyedMessage<String, String>(kafka_name, message);
		producer.send(data);
	}
	
	public void close(){
		if(producer != null){
			producer.close();
		}
	}
}
