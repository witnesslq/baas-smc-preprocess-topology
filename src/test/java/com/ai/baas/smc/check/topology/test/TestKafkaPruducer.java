package com.ai.baas.smc.check.topology.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public void send(String path) {
        InputStream in = null;
        BufferedReader reader = null;
        File file;
        try {
            file = FileUtils.getFile(path);
            in = new FileInputStream(file);
            reader = new BufferedReader(new InputStreamReader(in, Charsets.toCharset(encoding)));
            String sLine = reader.readLine();
            String batchNo = sLine.split(",")[1];
            String totalNum = sLine.split(",")[3];
            sLine = reader.readLine();
            while (sLine != null) {
                sLine = reader.readLine();
                if (sLine == null) {
                    break;
                }
                String message = "MVNE" + "MSG" + "MSG" + batchNo + FIELD_SPLIT + totalNum
                        + FIELD_SPLIT + assembleMessage(sLine);
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
