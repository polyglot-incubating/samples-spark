package org.chiwooplatform.samples.file;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.chiwooplatform.samples.support.RddFunction;
import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.junit.Test;

/**
 * val dataFrame =spark.read.csv("example.csv")
 * 
 * 
 * val dataFrame = spark.read.parquet("example.parquet")
 */
public class CsvExample /* implements Serializable */ {

    // private static final long serialVersionUID = -5073337853202330878L;

    /* 구분자가 !?! 인 경우 */
    private static final String DELIMITER = "\\!\\?\\!";

    @Test
    public void ut1001_samplesCSV() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("SparkCSVApp");
        Dataset<Row> rdd = spark.read().csv("src/main/resources/samples.csv");
        rdd.show();
        spark.close();
    }

    @Test
    public void ut1002_samplesCSVWithHeader() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("SparkCSVApp");
        Dataset<Row> rdd = spark.read().format("com.databricks.spark.csv").option("inferSchema", "true")
                .option("delimiter", ",").option("header", "true").load("src/main/resources/samples.csv");
        rdd.show();
        spark.close();
    }

    /**
     * Spark 의 csv reader 의 기본 옵션의 delimiter 구분자는 character 타입만 가능 하다.
     */
    @Test(expected = IllegalArgumentException.class)
    public void ut1003_customDilimeterException() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("SparkCSVApp");
        Dataset<Row> rdd = spark.read().format("com.databricks.spark.csv").option("inferSchema", "true")
                .option("delimiter", "!?!").option("header", "true")
                // .option("header", "false")
                .load("src/main/resources/samples.csv");

        rdd.show();
        spark.close();
    }

    @Test
    public void test_delimiter() throws Exception {
        String v = "201!?!301!?!4001!?!5001!?!Item1!?!201518!?!5";
        System.out.println(Arrays.toString(v.split("!?!")));
        System.out.println(Arrays.toString(v.split("\\!\\?\\!")));
    }

    // GC,AP2,AP1,ACCOUNT,Item,Week,Sell-out
    public static class Sellout {
        private String gc;
        private String ap2;
        private String api;
        private String account;
        private String item;
        private Integer weekno;
        private Integer sellout;

        @Override
        public String toString() {
            return "Sellout [gc=" + gc + ", ap2=" + ap2 + ", api=" + api + ", account=" + account + ", item=" + item
                    + ", weekno=" + weekno + ", sellout=" + sellout + "]";
        }

        public String getGc() {
            return gc;
        }

        public void setGc(String gc) {
            this.gc = gc;
        }

        public String getAp2() {
            return ap2;
        }

        public void setAp2(String ap2) {
            this.ap2 = ap2;
        }

        public String getApi() {
            return api;
        }

        public void setApi(String api) {
            this.api = api;
        }

        public String getAccount() {
            return account;
        }

        public void setAccount(String account) {
            this.account = account;
        }

        public String getItem() {
            return item;
        }

        public void setItem(String item) {
            this.item = item;
        }

        public Integer getWeekno() {
            return weekno;
        }

        public void setWeekno(Integer weekno) {
            this.weekno = weekno;
        }

        public Integer getSellout() {
            return sellout;
        }

        public void setSellout(Integer sellout) {
            this.sellout = sellout;
        }
    }

    private MapFunction<Row, Sellout> toSellout = row -> {
        String[] cols = row.getString(0).split(DELIMITER);
        Sellout so = new Sellout();
        int i = -1;
        so.setGc(cols[++i]);
        so.setAp2(cols[++i]);
        so.setApi(cols[++i]);
        so.setAccount(cols[++i]);
        so.setItem(cols[++i]);
        so.setWeekno(Integer.parseInt(cols[++i]));
        so.setSellout(Integer.parseInt(cols[++i]));
        return so;
    };

    /**
     * CSV 매트릭스 데이터를 JavaBeans 객체로 매핑
     * @throws Exception
     */
    @Test
    public void ut1004_customBeanWithDilimeter() throws Exception {
        // org.apache.spark.sql.catalyst.encoders.ExpressionEncoder.
        SparkSession spark = SparkContextHolder.getLocalSession("SparkCSVApp");
        Dataset<Sellout> rdd = spark.read().format("com.databricks.spark.csv").option("header", "true")
                .load("src/main/resources/samples2.csv").map(toSellout, Encoders.bean(Sellout.class)).cache();

        rdd.show();
        spark.close();
    }

    // private static MapFunction<Row, Row> buildRow = row -> {
    // final Object[] cols = row.getString(0).split(DILIMETER);
    // return RowFactory.create(cols);
    // };

    @Test
    public void ut1005_customDilimeterWithSchema() throws Exception {
        final String[] tuples = { "GC", "AP2", "AP1", "ACCOUNT", "ITEM", "WEEKNO", "SELLOUT" };
        final DataType[] types = { DataTypes.StringType, DataTypes.StringType, DataTypes.StringType,
                DataTypes.StringType, DataTypes.StringType, DataTypes.StringType, DataTypes.StringType };
        final StructType schema = SparkUtils.buildSchema(tuples, types);
        final SparkSession spark = SparkContextHolder.getLocalSession("SparkCSVApp");
        Dataset<Row> rdd = spark.read().csv("src/main/resources/samples3.csv")
                .map(RddFunction.rowByDelimiter(DELIMITER), RowEncoder.apply(schema)).cache();
        rdd.show();
        spark.close();
    }

    private static org.apache.spark.api.java.function.Function<Row, Row> buildJavaRow = row -> {
        final Object[] cols = row.getString(0).split(DELIMITER);
        return RowFactory.create(cols);
    };

    @Test
    public void ut1006_customDilimeterWithSchemaJavaRDD() throws Exception {
        final String[] tuples = { "GC", "AP2", "AP1", "ACCOUNT", "ITEM", "WEEKNO", "SELLOUT" };
        final DataType[] types = { DataTypes.StringType, DataTypes.StringType, DataTypes.StringType,
                DataTypes.StringType, DataTypes.StringType, DataTypes.StringType, DataTypes.StringType };
        final StructType schema = SparkUtils.buildSchema(tuples, types);
        final SparkSession spark = SparkContextHolder.getLocalSession("SparkCSVApp");
        JavaRDD<Row> javaRDD = spark.read().csv("src/main/resources/samples3.csv").toJavaRDD().map(buildJavaRow);
        System.out.println("CHECK JavaRDD DATA");
        javaRDD.take(10).forEach(System.out::println); 
        JavaPairRDD<Row, Long> jprdd = javaRDD.zipWithIndex();
        jprdd.take(10).forEach(System.out::println);
        Dataset<Row> rdd = spark.createDataFrame(javaRDD, schema);
        // drop rows for Scala
        // rdd.mapPartitions(iter -> iter.drop(1));
        rdd.show();
        spark.close();
    }

    @SuppressWarnings("serial")
    static FilterFunction<Row> rowFilter = new FilterFunction<Row>() {
        @Override
        public boolean call(Row row) throws Exception {
            final long rownum = row.getLong(1);
            return (rownum > 0);
        }
    };

    @Test
    public void ut1007_skipRowsInDataFrame() throws Exception {
        final String[] tuples = { "GC", "AP2", "AP1", "ACCOUNT", "ITEM", "WEEKNO", "SELLOUT" };
        final DataType[] types = { DataTypes.StringType, DataTypes.StringType, DataTypes.StringType,
                DataTypes.StringType, DataTypes.StringType, DataTypes.StringType, DataTypes.StringType };
        final StructType schema = SparkUtils.buildSchema(tuples, types);
        final SparkSession spark = SparkContextHolder.getLocalSession("SparkCSVApp");
// @formatter:off
        Dataset<Row> rdd = spark.read().csv("src/main/resources/samples3.csv")
                .withColumn("rownum", functions.monotonically_increasing_id())
                .filter(rowFilter)
                .limit(200)
                .map(RddFunction.rowByDelimiter(DELIMITER), RowEncoder.apply(schema)).cache();
        rdd.show();
        spark.close();
     // @formatter:on
    }

}
