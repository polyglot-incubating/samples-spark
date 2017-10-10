package org.chiwooplatform.samples.sql;

import java.util.Properties;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.chiwooplatform.samples.support.RddFunction;
import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.chiwooplatform.samples.support.StreamFunction;
import org.junit.Test;

public class ImportTranslatedData {

    private Properties props() throws IOException {
        InputStream in = ImportTranslatedData.class.getClassLoader()
                .getResourceAsStream("ImportTranslatedData.properties");
        Properties prop = new Properties();
        prop.load(in);
        in.close();
        return prop;
    }

    private static final char NL = '\n';

    private String createTABLE() {
        StringBuilder b = new StringBuilder();
        b.append("IF NOT EXISTS (SELECT 1 FROM sysObjects WHERE name='EXTBL01_ITEM_SALES' AND xtype='U')").append(NL);
        b.append("create table EXTBL01_ITEM_SALES (                  ").append(NL);
        b.append("    id      varchar(36)    NOT NULL,               ").append(NL);
        b.append("    name    varchar(100)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol1   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol2   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol3   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol4   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    kcol5   varchar(255)   COLLATE Korean_Wansung_CI_AS,").append(NL);
        b.append("    acol1   varchar(255)   COLLATE Latin1_General_CI_AS,").append(NL);
        b.append("    acol2   varchar(255)   COLLATE Latin1_General_CI_AS,").append(NL);
        b.append("    acol3   varchar(255)   COLLATE Latin1_General_CI_AS,").append(NL);
        b.append("    jcol1   varchar(255)   COLLATE Japanese_CI_AS, ").append(NL);
        b.append("    jcol2   varchar(255)   COLLATE Japanese_CI_AS, ").append(NL);
        b.append("    jcol3   varchar(255)   COLLATE Japanese_CI_AS, ").append(NL);
        b.append("    val1    INT, ").append(NL);
        b.append("    val2    INT, ").append(NL);
        b.append("    val3    INT, ").append(NL);
        b.append("    val4    INT, ").append(NL);
        b.append("    val5    INT, ").append(NL);
        b.append("    reg_dtm datetime        DEFAULT CURRENT_TIMESTAMP,  ").append(NL);
        b.append("    primary key(id) ").append(NL);
        b.append("); ").append(NL);
        return b.toString();
    }

    @Test
    public void ut1001_createTable() throws Exception {
        Connection conn = null;
        try {
            final Properties props = props();
            System.out.println(props.stringPropertyNames().toString());
            final String url = props.getProperty("url");
            conn = DriverManager.getConnection(url, props);
            Statement stmt = conn.createStatement();
            boolean result = stmt.execute(createTABLE());
            stmt.close();
            System.out.printf("result: %s, conn: %s", result, conn);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            conn.close();
        }
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

    @Test
    public void ut1002_checkDataFrame() throws Exception {
        final String filename = "src/main/resources/ItemSales.zip";
        JavaSparkContext ctx = SparkContextHolder.getLocalContext("checkDataFrame");
        // @formatter:off
        JavaRDD<Row> jrdd = ctx.binaryFiles(filename)
                .map(StreamFunction.unzipToString)
                .flatMap(RddFunction.toRowsByNewLine)
                .map(RddFunction.makeRowByDelimiter(","))
                .mapPartitionsWithIndex(RddFunction.skipRow(1), true);
        SparkSession spark = SparkContextHolder.getLocalSession("checkDataFrame");
        createDataset(spark, jrdd)
        .show(20); 
        // rdd.columns();
        // rdd.printSchema();
// @formatter:on
        spark.close();
        ctx.close();
    }

    /**
     * Import data to EX_TBL_TEMP Anonymous Table.
     * @throws Exception
     */
    @Test
    public void ut1003_importAnonymousTable() throws Exception {
        final Properties props = props();
        final String filename = "src/main/resources/ItemSales.zip";
        JavaSparkContext ctx = SparkContextHolder.getLocalContext("ImportTranslatedData");
        // @formatter:off
        JavaRDD<Row> jrdd = ctx.binaryFiles(filename)
                .map(StreamFunction.unzipToString)
                .flatMap(RddFunction.toRowsByNewLine)
                .map(RddFunction.makeRowByDelimiter(","))
                .mapPartitionsWithIndex(RddFunction.skipRow(1), true);
// @formatter:on
        SparkSession spark = SparkContextHolder.getLocalSession("ImportTranslatedData");
        Dataset<Row> rdd = createDataset(spark, jrdd);
        rdd.write().mode(SaveMode.Overwrite).jdbc(props.getProperty("url"), "EX_TBL_TEMP", props);
        spark.close();
        ctx.close();
    }

    /**
     * Import data to EXTBL01_ITEM_SALES Table.
     */
    @Test
    public void ut1004_importUserTable() throws Exception {
        createTABLE();
        final Properties props = props();
        final String filename = "src/main/resources/ItemSales.zip";
        JavaSparkContext ctx = SparkContextHolder.getLocalContext("import_EXTBL01_ITEM_SALES");
        // @formatter:off
        JavaRDD<Row> jrdd = ctx.binaryFiles(filename)
                .map(StreamFunction.unzipToString)
                .flatMap(RddFunction.toRowsByNewLine)
                .map(RddFunction.makeRowByDelimiter(","))
                .mapPartitionsWithIndex(RddFunction.skipRow(1), true);

        ut1001_createTable();
        SparkSession spark = SparkContextHolder.getLocalSession("import_EXTBL01_ITEM_SALES");
        spark.udf().register("UUID", (String val) -> SparkUtils.uuid(), DataTypes.StringType);
        Dataset<Row> rdd = createDataset(spark, jrdd).withColumnRenamed( "module", "name" )
                .withColumnRenamed( "gcode", "kcol1" )
                .withColumnRenamed( "acode", "kcol2" )
                .withColumnRenamed( "year", "val1" )
                .withColumnRenamed( "weekno", "val2" )
                .withColumnRenamed( "sellcnt", "val3" );

        Dataset<Row> result = rdd.select("name", "kcol1", "kcol2", "val1", "val2", "val3" )
                .withColumn("id", functions.callUDF("UUID", rdd.col("name")));
        // result.show(20);        
        result
            // .limit(50)
            .write()
            .mode(SaveMode.Append)
            .jdbc(props.getProperty("url"), "EXTBL01_ITEM_SALES", props);
// @formatter:on
        spark.close();
        ctx.close();
    }

    /**
     * Load from EXTBL01_ITEM_SALES Table.
     */
    @Test
    public void ut1005_loadFromUserTable() throws Exception {
        final Properties props = props();
        SparkSession spark = SparkContextHolder.getLocalSession("load_EXTBL01_ITEM_SALES");
        // @formatter:off
        spark.read()
            .format("jdbc").option("url", props.getProperty("url"))
            .option("dbtable", "dbo.EXTBL01_ITEM_SALES")
            .option("user", props.getProperty("user"))
            .option("password", props.getProperty("password"))
            .load()
            .createTempView("ITEM_SALES");
        Dataset<Row> rdd  = spark.sql("select id, name, kcol1, kcol2, val1 as yuear, val2 as weekno, val3 as sellcnt, reg_dtm from ITEM_SALES where val2 > 10");
        rdd.limit(2000);
        System.out.println("rdd.count(): " + rdd.count());
// @formatter:on
    }

}