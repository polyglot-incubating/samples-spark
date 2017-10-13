package org.chiwooplatform.samples.ml.binary;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.Binarizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.StructBuilder;
import org.junit.Test;

/**
 * 특정 조건(TCA)에 따른 true / false 를 판단 하여 target 을 계산 한다. 
 */
public class BinarizerExample {

    @Test
    public void ut1001_simpleBinaryTest() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("BinarizerExample");

        List<Row> data = Arrays.asList(
                RowFactory.create(0, 0.1), 
                RowFactory.create(1, 0.8), 
                RowFactory.create(2, 0.2), 
                RowFactory.create(3, 2.2), 
                RowFactory.create(5, 0.5),
                RowFactory.create(6, 1111.0),
                RowFactory.create(7, -0.5),
                RowFactory.create(9, -1.0)
            );
        StructType schema = new StructBuilder()
                .field("id").type(DataTypes.IntegerType).nullable(false).add()
                .field("value").type(DataTypes.DoubleType).nullable(false).add()
                .build();                
        Dataset<Row> continuousDataFrame = spark.createDataFrame(data, schema);

        Binarizer binarizer = new Binarizer().setInputCol("value").setOutputCol("target")
                .setThreshold(0.5);

        Dataset<Row> binarizedDataFrame = binarizer.transform(continuousDataFrame);

        System.out.println("Binarizer output with Threshold = " + binarizer.getThreshold());
        binarizedDataFrame.show();

        spark.close();
    }

}
