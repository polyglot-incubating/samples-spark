package org.chiwooplatform.samples.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
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
 * 된다. 참고)
 * https://jaceklaskowski.gitbooks.io/mastering-apache-spark/spark-tips-and-tricks-running-spark-windows.html
 * https://github.com/steveloughran/winutils
 */
public class FileWriteExample {

    private static final Pattern COLON = Pattern.compile(":");

    private List<Row> rowData() {
        List<Row> data = new ArrayList<>();
        data.add(RowFactory.create("name:Aider", "age:21", "gender:M"));
        data.add(RowFactory.create("name:Hongildong", "age:11", "gender:M"));
        data.add(RowFactory.create("name:Seohee", "age:26", "gender:F"));
        data.add(RowFactory.create("name:Emma", "age:51", "gender:F"));
        return data;
    }

    @SuppressWarnings("serial")
    private static final FlatMapFunction<Row, Row> func = new FlatMapFunction<Row, Row>() {

        @Override
        public Iterator<Row> call(Row row) throws Exception {
            final String[] col1 = COLON.split(row.getString(0));
            final String[] col2 = COLON.split(row.getString(1));
            final String[] col3 = COLON.split(row.getString(2));
            final Row newRow = RowFactory.create(col1[1], Integer.parseInt(col2[1]), col3[1]);
            return Arrays.asList(newRow).iterator();
        }
    };

    @SuppressWarnings("serial")
    private static final FlatMapFunction<Row, Row> mergeCols = new FlatMapFunction<Row, Row>() {

        @Override
        public Iterator<Row> call(Row row) throws Exception {
            final Row newRow = RowFactory.create(row.mkString(":"));
            return Arrays.asList(newRow).iterator();
        }
    };

    @Test
    public void test_createDatasetFromCollection() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("malFormedJsonCustomDatasetParser");

        StructType schema1 = SparkUtils.buildSchema(new String[] { "col1", "col2", "col3" },
                new DataType[] { DataTypes.StringType, DataTypes.StringType, DataTypes.StringType });
        StructType schema2 = SparkUtils.buildSchema(new String[] { "name", "age", "gender" },
                new DataType[] { DataTypes.StringType, DataTypes.IntegerType, DataTypes.StringType });
        Dataset<Row> ds = spark.createDataFrame(rowData(), schema1).flatMap(func, RowEncoder.apply(schema2));
        ds.show();
        spark.close();
    }

    String textfile = "target/spark/test.collection.txt";
    String jsonfile = "target/spark/test.collection.json";
    String csvfile = "target/spark/test.collection.csv";
    String parquetfile = "target/spark/test.collection.parquet";

    private void cleansingFiles() {
        FileUtils.deleteQuietly(new File(jsonfile));
        FileUtils.deleteQuietly(new File(csvfile));
        FileUtils.deleteQuietly(new File(parquetfile));
        FileUtils.deleteQuietly(new File(textfile));
    }

    @Test
    public void test_writeFileFromDataset() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("malFormedJsonCustomDatasetParser");
        StructType schema1 = SparkUtils.buildSchema(new String[] { "col1", "col2", "col3" },
                new DataType[] { DataTypes.StringType, DataTypes.StringType, DataTypes.StringType });
        StructType schema2 = SparkUtils.buildSchema(new String[] { "name", "age", "gender" },
                new DataType[] { DataTypes.StringType, DataTypes.IntegerType, DataTypes.StringType });
        Dataset<Row> ds = spark.createDataFrame(rowData(), schema1).flatMap(func, RowEncoder.apply(schema2));
        ds.show();
        this.cleansingFiles();
        ds.write().json(jsonfile);
        ds.write().csv(csvfile);
        ds.write().parquet(parquetfile);

        /**
         * text 파일 저장은 하나의 칼럼에 대해서만 가능 하다.
         */
        Dataset<Row> dsTxt = ds.flatMap(mergeCols, RowEncoder.apply(SparkUtils.stringSchema("value")));
        dsTxt.show();
        dsTxt.write().text(textfile);
        spark.close();
    }

}
