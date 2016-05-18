package com.ai.baas.smc.preprocess.topology.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ai.opt.base.exception.SystemException;
import com.ai.opt.sdk.constants.SDKConstants;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class HbaseClient {

    private static Logger logger = LoggerFactory.getLogger(HbaseClient.class);

    public static final String FIELD_SPLIT = new String(new char[] { (char) 1 });

    private static Connection connection;

    private static Properties prop = new Properties();

    private static Configuration conf;

    private static HbaseClient hbaseClient;

    public static HbaseClient getInstance() {
        if (hbaseClient == null) {
            loadResource();
        }
        hbaseClient = new HbaseClient();
        return hbaseClient;
    }

    public Connection getConnection() {
        if (connection == null) {
            loadResource();
        }
        return connection;
    }

    private synchronized static void loadResource() {
        InputStream is = HbaseClient.class.getClassLoader().getResourceAsStream(
                SDKConstants.PAAS_CONFIG_FILE);
        try {
            prop.load(is);
        } catch (IOException e1) {
            throw new SystemException(e1);
        }

        conf = HBaseConfiguration.create();
        String hbaseSite = (String) prop.get("hbase.param");
        if (StringUtils.isBlank(hbaseSite)) {
            throw new SystemException("没有配置hbase.site属性信息!");
        }
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObject = (JsonObject) jsonParser.parse(hbaseSite);
        for (Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            conf.set(entry.getKey(), entry.getValue().getAsString());
        }
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new SystemException(e);
        }
    }

    /*
     * 创建表
     * 
     * @tableName 表名
     * 
     * @family 列族列表
     */
    public void creatTable(String tableName, String[] family) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = new HTableDescriptor(tableName);
        for (int i = 0; i < family.length; i++) {
            desc.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tableName)) {
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
    }

    /*
     * 为表添加数据（适合知道有多少列族的固定表）
     * 
     * @rowKey rowKey
     * 
     * @tableName 表名
     * 
     * @column1 第一个列族列表
     * 
     * @value1 第一个列的值的列表
     * 
     * @column2 第二个列族列表
     * 
     * @value2 第二个列的值的列表
     */
    private void addData(String rowKey, String tableName, String[] column1, String[] value1,
            String[] column2, String[] value2) throws IOException {
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
                                                                  // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("article")) { // article列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.add(Bytes.toBytes(familyName), Bytes.toBytes(column1[j]),
                            Bytes.toBytes(value1[j]));
                }
            }
            if (familyName.equals("author")) { // author列族put数据
                for (int j = 0; j < column2.length; j++) {
                    put.add(Bytes.toBytes(familyName), Bytes.toBytes(column2[j]),
                            Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        System.out.println("add data Success!");
    }

    /*
     * 根据rwokey查询
     * 
     * @rowKey rowKey
     * 
     * @tableName 表名
     */
    public static Result getResult(String tableName, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// 获取表
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        return result;
    }

    /*
     * 根据rwokey查询指定列的数据(支持分页)
     * 
     * @rowKey rowKey
     * 
     * @tableName 表名
     */
    public static ResultScanner getPartKeysResult(String familyName, String tableName,
            String rowKey, List<String> columns, int pageSize, int pageNum) throws IOException {
        Filter filter = new RowFilter(CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(rowKey)));
        Scan scan = new Scan();
        for (String column : columns) {
            scan.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column));
        }
        scan.setFilter(filter);
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等

        ResultScanner rs = table.getScanner(scan);

        for (Result r : rs) {
            System.out.println("rowkey:" + new String(r.getRow()));
            for (KeyValue keyValue : r.raw()) {
                System.out.println("列族:" + new String(keyValue.getFamily()) + " 列:"
                        + new String(keyValue.getQualifier()) + ":"
                        + new String(keyValue.getValue()));
            }
        }

        return rs;
    }

    /*
     * 遍历查询hbase表
     * 
     * @tableName 表名
     */
    private static void getResultScann(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:" + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
                    System.out.println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out.println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /*
     * 遍历查询hbase表
     * 
     * @tableName 表名
     */
    private static void getResultScann(String tableName, String start_rowkey, String stop_rowkey)
            throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            for (Result r : rs) {
                for (KeyValue kv : r.list()) {
                    System.out.println("row:" + Bytes.toString(kv.getRow()));
                    System.out.println("family:" + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
                    System.out.println("value:" + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp:" + kv.getTimestamp());
                    System.out.println("-------------------------------------------");
                }
            }
        } finally {
            rs.close();
        }
    }

    /*
     * 查询表中的某一列
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     */
    private static void getResultByColumn(String tableName, String rowKey, String familyName,
            String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }

    /*
     * 更新表中的某一列
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     * 
     * @familyName 列族名
     * 
     * @columnName 列名
     * 
     * @value 更新后的值
     */
    private static void updateTable(String tableName, String rowKey, String familyName,
            String columnName, String value) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }

    /*
     * 查询某列数据的多个版本
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     * 
     * @familyName 列族名
     * 
     * @columnName 列名
     */
    private static void getResultByVersion(String tableName, String rowKey, String familyName,
            String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
        /*
         * List<?> results = table.get(get).list(); Iterator<?> it = results.iterator(); while
         * (it.hasNext()) { System.out.println(it.next().toString()); }
         */
    }

    /*
     * 删除指定的列
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     * 
     * @familyName 列族名
     * 
     * @columnName 列名
     */
    private static void deleteColumn(String tableName, String rowKey, String falilyName,
            String columnName) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(falilyName + ":" + columnName + "is deleted!");
    }

    /*
     * 删除指定的列
     * 
     * @tableName 表名
     * 
     * @rowKey rowKey
     */
    private static void deleteAllColumn(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        System.out.println("all columns are deleted!");
    }

    /*
     * 删除表
     * 
     * @tableName 表名
     */
    private static void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + "is deleted!");
    }

    // 添加一条数据
    public static void addRow(String tableName, String row, String columnFamily, String[] column,
            String[] value) throws Exception {
        HTable table = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(row));// 指定行
        // 参数分别:列族、列、值
        for (int i = 0; i < column.length; i++) {
            put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column[i]), Bytes.toBytes(value[i]));
        }
        table.put(put);
    }

}
