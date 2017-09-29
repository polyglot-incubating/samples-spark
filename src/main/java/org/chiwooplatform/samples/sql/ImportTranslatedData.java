package org.chiwooplatform.samples.sql;

import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;

import javax.sound.midi.Soundbank;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.chiwooplatform.samples.support.RddFunction;
import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.chiwooplatform.samples.support.StreamFunction;
import org.junit.Test;

public class ImportTranslatedData {

    private static final char NL = '\n';

    private String tableDDL() {
        StringBuilder b = new StringBuilder();
        b.append("IF NOT EXISTS (SELECT 1 FROM sysObjects WHERE name='EX_TBL_01' AND xtype='U')").append(NL);
        b.append("create table EX_TBL_01 (                                ").append(NL);
        b.append("    id      varchar(36)    NOT NULL,                    ").append(NL);
        b.append("    name    varchar(100)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol1   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol2   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol3   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol4   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol5   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    acol1   varchar(255)   COLLATE Latin1_General_CI_AS,").append(NL);
        b.append("    acol2   varchar(255)   COLLATE Latin1_General_CI_AS,").append(NL);
        b.append("    acol3   varchar(255)   COLLATE Latin1_General_CI_AS,").append(NL);
        b.append("    jcol1   varchar(255)   COLLATE Japanese_CI_AS,      ").append(NL);
        b.append("    jcol2   varchar(255)   COLLATE Japanese_CI_AS,      ").append(NL);
        b.append("    jcol3   varchar(255)   COLLATE Japanese_CI_AS,      ").append(NL);
        b.append("    val1    INT,   ").append(NL);
        b.append("    val2    INT,   ").append(NL);
        b.append("    val3    INT,   ").append(NL);
        b.append("    val4    INT,   ").append(NL);
        b.append("    val5    INT,   ").append(NL);
        b.append("    reg_dtm datetime        DEFAULT CURRENT_TIMESTAMP,  ").append(NL);
        b.append("    primary key(id) ").append(NL);
        b.append("); ").append(NL);
        b.append("GO").append(NL);
        return b.toString();
    }

    private Properties props() {
        Properties prop = new Properties();
        prop.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        prop.setProperty("user", "aider");
        prop.setProperty("password", "aider1234");
        return prop;
    }

    // val prop = new java.util.Properties
    // prop.setProperty("driver", "com.mysql.jdbc.Driver")
    // prop.setProperty("user", "root")
    // prop.setProperty("password", "pw")

    @Test
    public void ut1001_createTable() throws Exception {

        Connection conn = null;
        String url = "jdbc:sqlserver://SeonBoShim\\MSSQLSERVER:1433";
        try { 
            conn = DriverManager.getConnection(url, props() );
            System.out.println("conn: " + conn);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            conn.close();
        }
        //
        //
        //
        // SparkSession spark = SparkContextHolder.getLocalSession("CreateTableSample");
        // //
        // jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]
        // Dataset<Row> jdbcDF = spark.read().format("jdbc").option("url",
        // "jdbc:sqlserver://SeonBoShim")
        // .option("dbtable", "dbo.SAMPLE01").option("user", "aider").option("password",
        // "aider1234").load();
        // // spark.read().format("jdbc").option("url", "jdbc:sqlserver://SeonBoShim")
        // // .option("dbtable", "dbo.SAMPLE01").option("user",
        // "aider").option("password",
        // // "aider1234").load().write().mode(saveMode)
    }

    /**
     * 
     * <pre>
     * ItemSales.zip Schema
     * gcode acode module year weekno sellcnt
     * </pre>
     * 
     * @param spark
     * @param rdd
     * @return
     */
    private Dataset<Row> createDataset(final SparkSession spark, final JavaRDD<Row> rdd) {
        final String[] tuples = { "gcode", "acode", "module", "year", "weekno", "sellcnt" };
        final DataType[] dataTypes = { DataTypes.StringType, DataTypes.StringType, DataTypes.StringType,
                DataTypes.StringType, DataTypes.StringType, DataTypes.StringType };
        final StructType schema = SparkUtils.buildSchema(tuples, dataTypes);
        Dataset<Row> result = spark.createDataFrame(rdd, schema);
        return result;
    }

    private void importData(final SparkSession spark, final Dataset<Row> rdd) {
        Properties props = new Properties();
        props.setProperty("driver", "");
        props.setProperty("user", "aider");
        props.setProperty("password", "aider1234");
        props.setProperty("url", "aider1234");

        final String url = props.getProperty("url");

        rdd.write().mode(SaveMode.Overwrite).jdbc(url, "table", props);
    }

    /**
     * 
     * @throws Exception
     */
    @Test
    public void test_importItemSalesData() throws Exception {
        final String filename = "src/main/resources/ItemSales.zip";

        JavaSparkContext ctx = SparkContextHolder.getLocalContext("ImportTranslatedData");
        JavaRDD<Row> jrdd = ctx.binaryFiles(filename).map(StreamFunction.compressedStreamToString)
                .flatMap(RddFunction.toRowsByNewLine).map(RddFunction.makeRowByDelimiter(","));
        // System.out.println("jrdd.count(): " + jrdd.count());
        // SparkUtils.log(jrdd.take(10));
        SparkSession spark = SparkContextHolder.getLocalSession("ImportTranslatedData");
        Dataset<Row> rdd = createDataset(spark, jrdd);
        // rdd.printSchema();
        rdd.show(20);
        ctx.close();
    }
}

// @formatter:off
///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *    http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package buri.sparkour;
//
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.util.Properties;
//
///**
// * Loads a DataFrame from a relational database table over JDBC,
// * manipulates the data, and saves the results back to a table.
// */
//public final class JUsingJDBC {
//
//    public static void main(String[] args) throws Exception {
//        SparkSession spark = SparkSession.builder().appName("JUsingJDBC").getOrCreate();
//
//        // Load properties from file
//        Properties dbProperties = new Properties();
//        dbProperties.load(new FileInputStream(new File("db-properties.flat")));
//        String jdbcUrl = dbProperties.getProperty("jdbcUrl");
//
//        System.out.println("A DataFrame loaded from the entire contents of a table over JDBC.");
//        String where = "sparkour.people";
//        Dataset<Row> entireDF = spark.read().jdbc(jdbcUrl, where, dbProperties);
//        entireDF.printSchema();
//        entireDF.show();
//
//        System.out.println("Filtering the table to just show the males.");
//        entireDF.filter("is_male = 1").show();
//
//        System.out.println("Alternately, pre-filter the table for males before loading over JDBC.");
//        where = "(select * from sparkour.people where is_male = 1) as subset";
//        Dataset<Row> malesDF = spark.read().jdbc(jdbcUrl, where, dbProperties);
//        malesDF.show();
//
//        System.out.println("Update weights by 2 pounds (results in a new DataFrame with same column names)");
//        Dataset<Row> heavyDF = entireDF.withColumn("updated_weight_lb", entireDF.col("weight_lb").plus(2));
//        Dataset<Row> updatedDF = heavyDF.select("id", "name", "is_male", "height_in", "updated_weight_lb")
//            .withColumnRenamed("updated_weight_lb", "weight_lb");
//        updatedDF.show();
//
//        System.out.println("Save the updated data to a new table with JDBC");
//        where = "sparkour.updated_people";
//        updatedDF.write().mode("error").jdbc(jdbcUrl, where, dbProperties);
//
//        System.out.println("Load the new table into a new DataFrame to confirm that it was saved successfully.");
//        Dataset<Row> retrievedDF = spark.read().jdbc(jdbcUrl, where, dbProperties);
//        retrievedDF.show();
//
//        spark.stop();
//    }
//}
 
// @formatter:on
