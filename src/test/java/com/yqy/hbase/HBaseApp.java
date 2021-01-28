package com.yqy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;


/**
 * @author bahsk
 * @createTime 2021-01-20 21:43
 * @description
 */
public class HBaseApp {

    Connection connection = null;
    Table tb = null;
    Admin admin = null;

    String tableName  = "students_1";

    @Before
    public void setUp() {
        Configuration configuration = new Configuration();

        configuration.set("hbase.rootdir","hdfs://bigdata244:8020/hbase");
        configuration.set("hbase.zookeeper.quorum","bigdata244:2181");

        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();

            Assert.assertNotNull(configuration);
            Assert.assertNotNull(admin);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void getConnection() throws IOException {

    }

    @Test
    public void createTable() throws IOException {
        TableName table = TableName.valueOf(tableName);
        if(admin.tableExists(table)){
            System.out.println(tableName + "已经存在了...");
        }else {
            //HBase 旧版写法
            //HTableDescriptor descriptor = new HTableDescriptor(table);
            //表描述器构造器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);
            //列族描述器构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));
            //获得列描述器
            ColumnFamilyDescriptor columnFamilyDescriptor = cdb.build();
            //添加列族
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            //获得表描述器
            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            //创建表
            admin.createTable(tableDescriptor);

            if(admin.tableExists(table)){
                System.out.println(tableName + "创建成功了...");
            }


        }
    }

    /**
     * @author: bahsk
     * @date: 2021/1/28 21:33
     * @description: queryTableInfos 查询目前数据库中的表名
     * @params:
     * @return:
     */
    @Test
    public void queryTableInfos() throws IOException {
        List<TableDescriptor> tableDescriptorList = admin.listTableDescriptors();
        if(tableDescriptorList.size() > 0){
            for(TableDescriptor tableDescriptor : tableDescriptorList) {
                TableName tName = tableDescriptor.getTableName();
                System.out.println("table Priority" + tableDescriptor.getPriority() + ":" + tName.getNameAsString());
            }
        }
    }

     /**
      * @author: bahsk
      * @date: 2021/1/28 21:35
      * @description: testPut
      * @params:
      * @return:
      */
    @Test
    public void testPut() throws IOException {
        tb =  connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes("learn"));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("yqy"));
        tb.put(put);
        TableDescriptor tableDescriptor = admin.getDescriptor(TableName.valueOf(tableName));
        System.out.println(tableDescriptor.getValue("name"));

    }



    @After
    public void tearDown(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
