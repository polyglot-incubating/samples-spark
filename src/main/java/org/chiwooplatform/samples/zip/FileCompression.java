package org.chiwooplatform.samples.zip;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.chiwooplatform.samples.support.RddFunction;
import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.chiwooplatform.samples.support.StreamFunction;
import org.chiwooplatform.samples.support.handler.AbstractJsonRowHandler;
import org.junit.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

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

    private StructType schema() {
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

    /**
     * <code>
    {"ID":4,"NAME":"Emma","AGE":"51","GENDER":"F"}
    {"ID":5,"NAME":"Tiger","AGE":"49","GENDER":"M"}
    {"ID":6,"NAME":"Scott","AGE":"53","GENDER":"M"}</code>
     */
    public static class SimpleUser implements Serializable {
        private static final long serialVersionUID = 4501836450282174867L;
        // {"ID":2,"NAME":"Hongildong","AGE":"11","GENDER":"M"}
        @JsonProperty("ID")
        private Integer id;
        @JsonProperty("NAME")
        private String name;
        @JsonProperty("AGE")
        private String age;
        @JsonProperty("GENDER")
        private String gender;

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

    }

    @SuppressWarnings("serial")
    private static final AbstractJsonRowHandler callback = new AbstractJsonRowHandler() {
        @Override
        protected Row makeRow(ObjectMapper mapper, String value) throws Exception {
            final SimpleUser user = mapper.readValue(value, SimpleUser.class);
            final Row row = RowFactory.create(user.getId(), user.getName(), user.getAge(), user.getGender());
            return row;
        }
    };

    @Test
    public void test_readFromUnCompressFile() throws Exception {

        final String filename = "target/spark/compression.collection.gz/*.gz";
        JavaSparkContext spark = SparkContextHolder.getLocalContext("UnCompressFile");
        final JavaRDD<Row> jrdd = spark.binaryFiles(filename).map(StreamFunction.dataStreamToString)
                .flatMap(v -> Arrays.asList(v.split("\\n")).iterator())
                .mapPartitions(RddFunction.flatMapPartitions(callback));

        // System.out.println("count: " + jrdd.count());
        SparkUtils.log(jrdd.take(10));

        // 중요!!! RDD 를 활용 하기 위해선 Session 이 열려 있어야 한다.
        // spark.close();
        // SparkUtils.log(jrdd.take(10));

        final StructType schema = SparkUtils.buildSchema(new String[] { "ID", "NAME", "AGE", "GENDER" },
                new DataType[] { DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType,
                        DataTypes.StringType });

        SparkSession session = SparkContextHolder.getLocalSession("DatasetUnCompressFile");
        Dataset<Row> rdd = session.createDataFrame(jrdd, schema);

        rdd.printSchema();
        rdd.show();
        rdd.createOrReplaceTempView("SimpleUSER");
        Dataset<Row> result = session.sql("SELECT * FROM SimpleUSER where GENDER = 'F'");
        result.show();
        session.close();
        spark.close();
    }
}
