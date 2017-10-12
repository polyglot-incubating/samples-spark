package org.chiwooplatform.samples.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * val dframe = spark.jsonFile("hdfs:///data/spark/device.json")
 */
public class JsonExample {

    @Test
    public void ut1001_loadFormedJson() throws Exception {
        final Pattern NL = Pattern.compile("\\n");

        final String json = "{\"id\":4,\"name\":\"eMMA\",\"age\":\"51\",\"gender\":\"f\"}" + "\n"
                + "{\"id\":5,\"name\":\"tIGER\",\"age\":\"49\",\"gender\":\"m\"}" + "\n"
                + "{\"id\":6,\"name\":\"sCOTT\",\"age\":\"53\",\"gender\":\"m\"}" + "\n";

        JavaSparkContext spark = SparkContextHolder.getLocalContext("JsonExample");
        JavaRDD<String> jrdd = spark.parallelize(Arrays.asList(NL.split(json)));
        // System.out.println("jrdd.count() : " + jrdd.count());
        // SparkUtils.log(jrdd.take(10));

        SparkSession session = SparkContextHolder.getLocalSession("JsonExample2");
        @SuppressWarnings("deprecation")
        Dataset<Row> rdd = session.read().json(jrdd);
        rdd.show();
        session.close();
        spark.close();
    }

    @Test
    public void ut1002_wellFormedJson() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("JsonExample");
        Dataset<Row> rdd = spark.read().json("src/main/resources/json-well-formed.json");
        rdd.show();
        spark.close();
    }

    /**
     * MAL-FORMED-JSON 은 자료구조가 DOM Tree와 같아서 대용량 bulk 로 적재하여 처리하긴 적절치 않다.
     * 
     * 일단은, Query 로 임시적인 해결을 한다.
     * @throws Exception
     */
    @Test
    public void ut1003_malFormedJson() throws Exception {
        SparkSession spark = SparkContextHolder.getLocalSession("JsonExample");

        Column name = SparkUtils.column("name");
        Column no = SparkUtils.column("no");
        Column points = SparkUtils.column("points");
        Column dict = SparkUtils.column("dict");
        Dataset<Row> rdd = spark.read().json("src/main/resources/json-mal-formed.json").select(name, no, points, dict);
        rdd.createOrReplaceTempView("mal");
        rdd.printSchema();
        rdd.show();

        Dataset<Row> result = spark.sql("select * from mal where name is not null");
        result.show();
        spark.close();
    }

    // {"name":"name 1","no":1,"points":[1,2,3],"dict": {"key": "value1"}},
    @SuppressWarnings("serial")
    public static class Person implements Serializable {
        private String name;
        private Integer no;
        private List<Integer> points;
        private Map<String, String> dict;

        @Override
        public String toString() {
            return "Person [name=" + name + ", no=" + no + ", points=" + points + ", dict=" + dict + "]";
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getNo() {
            return no;
        }

        public void setNo(Integer no) {
            this.no = no;
        }

        public List<Integer> getPoints() {
            return points;
        }

        public void setPoints(List<Integer> points) {
            this.points = points;
        }

        public Map<String, String> getDict() {
            return dict;
        }

        public void setDict(Map<String, String> dict) {
            this.dict = dict;
        }

    }

    /**
     * line 단위의 Custom Json Parser 를 구현 하는 단순한 예제
     */
    @SuppressWarnings("serial")
    static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
        final ObjectMapper mapper = new ObjectMapper();

        public Iterator<Person> call(Iterator<String> lines) throws Exception {
            ArrayList<Person> people = new ArrayList<Person>();
            while (lines.hasNext()) {
                final String line = lines.next().trim();
                if (line == null || line.length() < 3) {
                    continue;
                }
                try {
                    people.add(mapper.readValue(line, Person.class));
                }
                catch (Exception e) {
                    // skip records on failure
                    e.printStackTrace();
                }
            }
            return people.iterator();
        }
    }

    /**
     * MAL-FORMED-JSON 은 자료구조가 DOM Tree와 같아서 대용량 bulk 로 적재하여 처리하긴 적절치 않다.
     * 
     * CustomParser를 통한 처리
     * @throws Exception
     */
    @Test
    public void ut1004_malFormedJsonCustomParser() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("malFormedJsonCustomParser");
        JavaRDD<Person> rdd = spark.textFile("src/main/resources/json-mal-formed.json").mapPartitions(new ParseJson());
        System.out.println("rdd.count(): " + rdd.count());
        SparkUtils.log(rdd.take(10));
        spark.close();
    }

    @SuppressWarnings("serial")
    static class ParseJsonRow implements FlatMapFunction<Iterator<Row>, Person> {
        final ObjectMapper mapper = new ObjectMapper();

        @Override
        public Iterator<Person> call(Iterator<Row> rows) throws Exception {
            List<Person> result = new ArrayList<>();
            while (rows.hasNext()) {
                Row row = rows.next();
                final String line = row.getString(0).trim();
                if (line == null || line.length() < 3) {
                    continue;
                }
                try {
                    Person person = mapper.readValue(line, Person.class);
                    result.add(person);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return result.iterator();
        }
    }

    /**
     * SparkSession 을 직접적으로 다루는 방식 (JavaSparkContext를 거치지 않음)
     */
    @Test
    public void ut1005_malFormedJsonCustomDatasetParser() throws Exception {
        final SparkSession spark = SparkContextHolder.getLocalSession("malFormedJsonCustomDatasetParser");
        JavaRDD<Person> rdd = spark.read().text("src/main/resources/json-mal-formed.json").javaRDD()
                .mapPartitions(new ParseJsonRow());
        // System.out.println("rdd.count(): " + rdd.count());
        // SparkUtils.olog(rdd.take(10));
        Dataset<Row> ds = spark.createDataFrame(rdd, Person.class);
        ds.show();

        spark.read().text("src/main/resources/json-mal-formed.json").javaRDD().mapPartitions(new ParseJsonRow())
        // .flatMap(s -> spark.createDataFrame(s, Person.class) )
        ;

        // ds.write().parquet("target/spark/Person.Dataframe.parquet");
        /**
         * text 파일은 single 칼럼만 가능
         */
        // ds.write().text("target/spark/Person.Dataframe.txt");
        // ds.write().csv("target/spark/Person.Dataframe.csv");;

        // ds.javaRDD().saveAsTextFile("target/spark/Person.Dataframe.json");
        // ds.javaRDD().saveAsObjectFile("target/spark/Person.Dataframe.obj");
        spark.close();
    }

}
