package com.yqy.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
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

    String tableName  = "stu1";

    @Before
    public void setUp() {
        Configuration configuration = new Configuration();

        configuration.set("hbase.rootdir","hdfs://localhost:9000/hbase");
        configuration.set("hbase.zookeeper.quorum","127.0.0.1:2181");

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

     /**
      * @author: bahsk
      * @date: 2021/1/30 22:42
      * @description: 添加表,添加列族，若存在则删除表
      * @params:
      * @return:
      */
    @Test
    public void createTable() throws IOException {
        TableName rTableName = TableName.valueOf(tableName);
        if(admin.tableExists(rTableName)){
            System.out.println(rTableName + "已经存在了...");
            if(admin.isTableAvailable(rTableName)) {
                //要删除表首先需要禁用
                admin.disableTable(rTableName);
            }
            //deleteTable 就是drop表操作
            admin.deleteTable(rTableName);
        }else {
            //HBase 旧版写法
            //HTableDescriptor descriptor = new HTableDescriptor(table);
            //表描述器构造器
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(rTableName);
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


            if(admin.tableExists(rTableName)){
                System.out.println(rTableName + "创建成功了...");
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
      * @date: 2021/1/30 22:41
      * @description: 学习在已经使用的表中添加新的列族 ColumnFamilyDescriptorBuilder 2.0的方式
      *               老版本的HColumnDescriptor等已经过时
      * @params:
      * @return:
      */
    @Test
    public void testAddColumnFamily() throws IOException {
        //添加新的列族元素

        ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("others"));
        ColumnFamilyDescriptor columnFamilyDescriptor = cdb.build();
        //禁用表
        admin.disableTable(TableName.valueOf(tableName));
        //添加列族元素
        admin.addColumnFamily(TableName.valueOf(tableName), columnFamilyDescriptor);
        //启用表
        admin.enableTableAsync(TableName.valueOf(tableName));

        TableDescriptor tableDescriptor = admin.getDescriptor(TableName.valueOf(tableName));

        Assert.assertEquals(3, tableDescriptor.getColumnFamilyCount());
    }

     /**
      * @author: bahsk
      * @date: 2021/1/28 21:35
      * @description: testPut put可以新增列和列对应的值，
      *               但是不能添加列族,练习添加单条和多条
      *               List<Put> puts = new ArrayList<Put>();
      *               table.put(puts);
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
        Put put1 = new Put(Bytes.toBytes("cat001"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("yqy"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("1"));
        //为新的列族追加
        put1.addColumn(Bytes.toBytes("others"), Bytes.toBytes("type"), Bytes.toBytes("cat"));

//        ResultScanner resultScanner = table.getScanner(Bytes.toBytes("info"), Bytes.toBytes("name"));
//        Assert.assertNotNull(resultScanner);
        Put put2 = new Put(Bytes.toBytes("tom002"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("yqy"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("1"));
        //为新的列族追加
        put2.addColumn(Bytes.toBytes("others"), Bytes.toBytes("type"), Bytes.toBytes("cat"));

        puts.add(put1);
        puts.add(put2);

        table.put(puts);
    }


     /**
      * @author: bahsk
      * @date: 2021/1/30 22:39
      * @description:
      * @params:
      * @return:
      */
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

     /**
      * @author: bahsk
      * @date: 2021/1/30 22:39
      * @description:
      * @params:
      * @return:
      */
    @Test
    public void testScan() throws IOException {
        table =  connection.getTable(TableName.valueOf(tableName));
        //Scan是全表扫描  Scan scan = new Scan(); 默认是全表扫描
        //传入行键
        Scan scan = new Scan(new Get(Bytes.toBytes("learn001")));
        //根据需要的列族进行查询
        scan.addFamily(Bytes.toBytes("info"));
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            printResult(result);
        }
    }

     /**
      * @author: bahsk
      * @date: 2021/1/30 22:38
      * @description: 测试RowFilter + RegexStringComparator正则匹配
      * @params:
      * @return:
      */
    @Test
    public void testFilter() throws IOException {
        table =  connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //根据行键过滤 EQUAL LESS NOT_EQUAL GREATER_OR_EQUAL GREATER NO_OP
        String reg = "^cat";
        //RowFilter可以使用正则
        Filter filter = new RowFilter(CompareOperator.GREATER, new RegexStringComparator(reg));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            printResult(result);
        }
    }

     /**
      * @author: bahsk
      * @date: 2021/1/30 22:38
      * @description: 测试前缀过滤器 PrefixFilter
      * @params:
      * @return:
      */
    @Test
    public void testPrefixFilter() throws IOException {
        table =  connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String reg = "cat";
        //前缀目前经过测试不能使用正则
        Filter filter = new PrefixFilter(Bytes.toBytes(reg));
        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            printResult(result);
        }
    }

     /**
      * @author: bahsk
      * @date: 2021/1/30 22:25
      * @description: 测试FilterList ，MUST_PASS_ONE 和 MUST_PASS_ALL两种方式
      * @params:
      * @return:
      */
    @Test
    public void testFilterList() throws IOException {
        table =  connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String reg = "cat";
        //public FilterList(final Operator operator, final List<Filter> filters)
//        public enum Operator {
//            /** !AND */
//            MUST_PASS_ALL,  类似 AND
//            /** !OR */
//            MUST_PASS_ONE  类似 OR
//        }
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        Filter filter1 = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("^*002"));
        Filter filter2 = new PrefixFilter(Bytes.toBytes("ca"));
        Filter filter3 = new PrefixFilter(Bytes.toBytes("tom"));
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        filterList.addFilter(filter3);

        scan.setFilter(filterList);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner){
            printResult(result);
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
