package org.chiwooplatform.samples.zip;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.chiwooplatform.samples.SparkContextHolder;
import org.chiwooplatform.samples.SparkUtils;
import org.junit.Test;

public class FileCompression {
    private List<Row> rowData() {
        List<Row> data = new ArrayList<>();
        data.add(RowFactory.create(1, "Aider", "21", "M"));
        data.add(RowFactory.create(2, "Hongildong", "11", "M"));
        data.add(RowFactory.create(3, "Seohee", "23", "F"));
        data.add(RowFactory.create(4, "Emma", "51", "F"));
        data.add(RowFactory.create(5, "Tiger", "49", "M"));
        data.add(RowFactory.create(6, "Scott", "53", "M"));
        return data;
    }

    String gzfile = "target/spark/compression.collection.gz";

    StructType schema() {
        return SparkUtils.buildSchema(new String[] { "ID", "NAME", "AGE", "GENDER" }, new DataType[] {
                DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType, DataTypes.StringType });

    }

    @Test
    public void test_writeJsonCompressFile() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("FileCompression");
        final StructType schema = schema();
        Dataset<Row> ds = spark.createDataFrame(rowData(), schema);
        ds.show();
        // FileUtils.deleteQuietly(new File(gzfile));
        ds.write().mode(SaveMode.Overwrite).format("json").option("compression", "gzip").mode(SaveMode.Overwrite)
                .save(gzfile);
        spark.close();
    }
}
