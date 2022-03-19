package com.xujingtian;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BaseTest {

    public static void main(String[] args) throws IOException {
        // 建立连接

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2.cluster-285604");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "emr-worker-2.cluster-285604:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("xujingtian:student");

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("name");
            hTableDescriptor.addFamily(hColumnDescriptor1);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("info");
            hTableDescriptor.addFamily(hColumnDescriptor2);
            HColumnDescriptor hColumnDescriptor3 = new HColumnDescriptor("score");
            hTableDescriptor.addFamily(hColumnDescriptor3);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        // 插入数据
        Put put1 = new Put(Bytes.toBytes("1")); // row key
        put1.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("Tom"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put1.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("75"));
        put1.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("82"));
        conn.getTable(tableName).put(put1);
        System.out.println("Data insert success");
        Put put2 = new Put(Bytes.toBytes("2")); // row key
        put2.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("Tom"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000002"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
        put2.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
        put2.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("67"));
        conn.getTable(tableName).put(put2);
        System.out.println("Data insert success");
        Put put3 = new Put(Bytes.toBytes("3")); // row key
        put3.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("Jack"));
        put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000003"));
        put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put3.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
        put3.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("80"));
        conn.getTable(tableName).put(put3);
        System.out.println("Data insert success");
        Put put4 = new Put(Bytes.toBytes("4")); // row key
        put4.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("Rose"));
        put4.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000004"));
        put4.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put4.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("60"));
        put4.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("61"));
        conn.getTable(tableName).put(put4);
        System.out.println("Data insert success");
        Put put5 = new Put(Bytes.toBytes("5")); // row key
        put5.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("xujingtian"));
        put5.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("G20210607040077"));
        put5.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
        put5.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
        put5.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("85"));
        conn.getTable(tableName).put(put5);
        System.out.println("Data insert success");

        // 查看数据
        Get get = new Get(Bytes.toBytes("5")); // 指定rowKey
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                        cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes("1")); // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}