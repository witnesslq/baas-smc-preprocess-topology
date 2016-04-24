package com.ai.baas.smc.check.topology.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;

/**
 * 
 * Date: 2016年3月24日 <br>
 * Copyright (c) 2016 asiainfo.com <br>
 * 
 * @author LiangMeng
 */
public class TestKafkaPruducer {
    private static Logger LOG = LoggerFactory.getLogger(TestKafkaPruducer.class);

    private String encoding = "gbk";

    public final static String FIELD_SPLIT = new String(new char[] { (char) 1 });

    public final static String RECORD_SPLIT = new String(new char[] { (char) 2 });

    public final static String PACKET_HEADER_SPLIT = ",";

    private String[] outputFields;

    private MappingRule[] mappingRules = new MappingRule[2];

    public void send(String path) {
        InputStream in = null;
        BufferedReader reader = null;
        File file;
        try {
            file = FileUtils.getFile(path);
            in = new FileInputStream(file);
            reader = new BufferedReader(new InputStreamReader(in, Charsets.toCharset(encoding)));
            String sLine = reader.readLine();
            // String batchNo = sLine.split(",")[1];
            // String totalNum = sLine.split(",")[3];
            sLine = reader.readLine();
            while (sLine != null) {
                sLine = reader.readLine();
                if (sLine == null) {
                    break;
                }
                // batch_no
                // total_record
                // msg_id
                // end_user
                // chnnel_id
                // tel_no
                // send_time
                // content
                // state
                // fee_item_id
                // total_fee
                String string = "MVNE\u0001msg\u0001msg\u000120160301001\u0001JSMVNE2016030100123hx\u000120160421162542\u0001201604\u0001JSMVNE20160301001\u0001100\u000123\u0001hx\u0001bw\u00011709123478\u000120160323\u0001\"\"\"testcontent\"\"\u0001\"\"testcontent\"\"\u0001\"\"testcontent\"\"\"\u00011";
                mappingRules[0] = MappingRule.getMappingRule(MappingRule.FORMAT_TYPE_OUTPUT,
                        BaseConstants.JDBC_DEFAULT);
                mappingRules[1] = mappingRules[0];
                MessageParser messageParser = MessageParser.parseObject(string, mappingRules,
                        outputFields);

                Map<String, String> data = messageParser.getData();
                String batchNo = data.get(SmcConstants.BATCH_NO);
                String totalRecord = data.get("total_record");
                String msgId = data.get("msg_id");
                String endUser = data.get("end_user");
                String chnnelId = data.get("chnnel_id");
                String telNo = data.get("tel_no");
                String sendTime = data.get("send_time");
                String content = data.get("content");
                String state = data.get("state");
                String feeItemId = data.get("fee_item_id");
                String totalFee = data.get("total_fee");
                // 租户0x01业务0x01来源0x01批次号0x01SN0x01账期0x01到达时间0x01
                // + batchNo + totalRecord + msgId + endUser + chnnelId + telNo
                // + sendTime + content + state + feeItemId + totalFee
                String message = "MVNE" + "MSG" + "MSG" + "JSMVNE" + batchNo + "sn" + "201603"
                        + "201603" + batchNo + totalRecord + msgId + endUser + chnnelId + telNo
                        + sendTime + content + state + feeItemId + totalFee + FIELD_SPLIT
                        + assembleMessage(sLine);
                LOG.info("message----" + message);
                ProducerProxy.getInstance().sendMessage(message);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    private String assembleMessage(String line) {
        String[] fieldNames = null;
        StringBuilder record = new StringBuilder();
        fieldNames = StringUtils.splitPreserveAllTokens(line, ",");
        for (String fieldName : fieldNames) {
            record.append(fieldName).append(FIELD_SPLIT);
        }
        return record.substring(0, record.length() - 1).toString();
    }

    public static void main(String[] args) {
        String path = "E:\\Baas\\短信业务流水数据_1.csv";
        TestKafkaPruducer simulator = new TestKafkaPruducer();
        simulator.send(path);
    }

}
