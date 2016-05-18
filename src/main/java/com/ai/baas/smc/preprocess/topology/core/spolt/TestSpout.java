package com.ai.baas.smc.preprocess.topology.core.spolt;

import java.util.Map;

import org.apache.hadoop.hbase.client.Connection;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.ai.baas.storm.util.HBaseProxy;

public class TestSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private static Connection connection;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
//        // TODO Auto-generated method stub
//        HBaseProxy.loadResource(conf);
//        connection = HBaseProxy.getConnection();
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        String string = "MVNE\u0001msg\u0001msg\u000120160301001\u0001JSMVNE2016030100123hx\u000120160421162542\u0001201604\u0001JSMVNE20160301001\u0001100\u000123\u0001hx\u0001bw\u00011709123478\u000120160323\u0001\"\"\"testcontent\"\"\u0001\"\"testcontent\"\"\u0001\"\"testcontent\"\"\"\u00011";
        collector.emit(new Values(string));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("source"));
    }

}
