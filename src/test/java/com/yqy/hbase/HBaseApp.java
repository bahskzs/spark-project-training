package com.yqy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.column.ColumnDescriptor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author bahsk
 * @createTime 2021-01-20 21:43
 * @description
 */
public class HBaseApp {

    Connection connection = null;
    Table table = null;
    Admin admin = null;

    String tableName  = "stu";

    @Before
    public void setUp() {
        Configuration configuration = new Configuration();

        configuration.set("hbase.rootdir","hdfs://218.85.80.113:15524/hbase");
        //configuration.set("hbase.zookeeper.quorum","192.168.10.18:2181");

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



    @Test
    public void testAddColumnFamily() throws IOException {
        //添加新的列族元素

        ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("Kates"));
        ColumnFamilyDescriptor columnFamilyDescriptor = cdb.build();
        //禁用表
        admin.disableTable(TableName.valueOf(tableName));
        //添加列族元素
        admin.addColumnFamily(TableName.valueOf(tableName), columnFamilyDescriptor);
        //启用表
        admin.enableTableAsync(TableName.valueOf(tableName));

        TableDescriptor tableDescriptor = admin.getDescriptor(TableName.valueOf(tableName));

        Assert.assertEquals(4, tableDescriptor.getColumnFamilyCount());
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

        table =  connection.getTable(TableName.valueOf(tableName));

//        Put put = new Put(Bytes.toBytes("learn"));
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("yqy"));
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("1"));
//        //为新的列族追加
//        put.addColumn(Bytes.toBytes("others"), Bytes.toBytes("type"), Bytes.toBytes("cat"));
        //table.put(put);


        //追加多条
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("learn001"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("yqy"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("1"));
        //为新的列族追加
        put1.addColumn(Bytes.toBytes("others"), Bytes.toBytes("type"), Bytes.toBytes("cat"));

//        ResultScanner resultScanner = table.getScanner(Bytes.toBytes("info"), Bytes.toBytes("name"));
//        Assert.assertNotNull(resultScanner);
        Put put2 = new Put(Bytes.toBytes("learn002"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("yqy"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("1"));
        //为新的列族追加
        put2.addColumn(Bytes.toBytes("others"), Bytes.toBytes("type"), Bytes.toBytes("cat"));

        puts.add(put1);
        puts.add(put2);

        table.put(puts);
    }


    @Test
    public void testGet() throws IOException {
        table =  connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes("learn001"));
        Result result = table.get(get);
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
//        List<Cell> cells = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("name"));
//        for(Cell cell : cells){
//            System.out.println(cell.);
//        }
        printResult(result);
    }

    private void printResult(Result result) {
       for(Cell cell : result.rawCells()) {
           /**
            * getRow()
            * Method for retrieving the row key that corresponds to
            * the row from which this Result was created.
            * @return row
            */
           System.out.println(Bytes.toString(result.getRow()) + "\t"
                   + Bytes.toString(CellUtil.cloneFamily(cell)) + "\t"
                   +  Bytes.toString(CellUtil.cloneQualifier(cell)) + "\t"
                   + Bytes.toString(CellUtil.cloneValue(cell)) + "\t"
                   + cell.getTimestamp());
       }
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
