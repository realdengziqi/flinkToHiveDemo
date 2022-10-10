package com.atguigu;



import org.apache.flink.streaming.api.datastream.DataStreamSource;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;

/**
 * @Description :
 * @Author:yujiahao
 * @Date: 2022/4/27 16:32
 * @Version: 1.0
 */
public class testFlinkSQL {
    public static void main(String[] args) throws Exception {
        //TODO 1、设置流环境
        System.setProperty("HADOOP_USER_NAME","atguigu");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setParallelism(4);



        //TODO 4、封装成流
//        DataStreamSource<String> streamSource = env.readTextFile("input/file.txt");
//
//        streamSource.print("测试通道是否打通");

        // 构造hive catalog
        String name = "myhive";//catalog的名称，定义一个唯一的一个标识
        String defaultDatabase = "default";//默认数据库名称
        String hiveConfDir = "src/main/resources"; // hive-site.xml路径
        String version = "2.1.1";//版本号

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        hive.getHiveConf().set("hive.metastore.uris","thrift://hadoop102:9083");
        tableEnv.registerCatalog("myhive", hive);
        tableEnv.useCatalog("myhive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.useDatabase("default");

//        tableEnv.createTemporaryView("users", streamSource);


//      在hive中创建表 测试数据
        String hiveSql = "create table yujiahao" +
                "(id int,name string,age int,gender string)" +
                "row format delimited fields terminated by '\\t'";
        try{
            tableEnv.executeSql(hiveSql);
        } catch (ValidationException e){
            if(e.getCause() instanceof  TableAlreadyExistException){
                // 如果表已经存在 就修改表的tbl属性
                String alterSql = "alter table yujiahao set TBLPROPERTIES ('sink.partition-commit.policy.kind'='metastore,success-file')";
                // 其实你也可以完全hive上改
                tableEnv.executeSql(alterSql);
            }else {
                throw e;
            }
        }
        String insertSql = "insert into yujiahao values(1,'梁朝伟',18,'男')";

        tableEnv.executeSql(insertSql);

    }
}

