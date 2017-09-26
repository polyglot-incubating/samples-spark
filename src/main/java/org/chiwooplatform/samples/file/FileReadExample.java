package org.chiwooplatform.samples.file;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.chiwooplatform.samples.SparkContextHolder;
import org.chiwooplatform.samples.SparkUtils;
import org.junit.Test;

/**
 * Windows 환경에서 Spark Dataset(DataFrame) 정보를 파일로 write 하려면
 * 
 * - Case1: Hadoop 전체를 Windows에 설치 하거나, - Case2: Hadoop 의 HDFS 라이브러리만 따로 Windows에 설치 하면
 * 된다.
 * 
 * 참고)
 * https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html
 * https://github.com/steveloughran/winutils
 */
public class FileReadExample {

    String textfile = "target/spark/test.collection.txt";
    String jsonfile = "target/spark/test.collection.json";
    String csvfile = "target/spark/test.collection.csv";
    String parquetfile = "target/spark/test.collection.parquet";

    static private StructType schema() {
        final StructType schema = SparkUtils.buildSchema(new String[] { "name", "age", "gender" },
                new DataType[] { DataTypes.StringType, DataTypes.StringType, DataTypes.StringType });
        return schema;
    }

    @Test
    public void test_createDatasetFromFiles() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("FileReadExample");

        System.out.println("---------------------- rddCSV.show() ----------------------");
        Dataset<Row> rddCSV = spark.createDataFrame(spark.read().csv(csvfile).collectAsList(), schema());
        rddCSV.show();

        System.out.println("---------------------- rddJSON.show() ----------------------");
        Dataset<Row> rddJSON = spark.read().json(jsonfile);
        rddJSON.show();

        System.out.println("---------------------- rddParquet.show() ----------------------");
        Dataset<Row> rddParquet = spark.read().parquet(parquetfile);
        rddParquet.show();

        System.out.println("---------------------- rddText.show() ----------------------");
        Dataset<Row> rddText = spark.read().text(textfile);
        rddText.show();

        spark.close();
    }

}
