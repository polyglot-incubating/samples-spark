package org.chiwooplatform.samples.rdd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PersonDataFrame {

    @SuppressWarnings("serial")
    public static class Person implements Serializable {
        private String name;
        private int age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person [name=" + name + ", age=" + age + "]";
        }

    }

    private static Dataset<Person> buildDF(SparkSession spark, final String jsonFilepath) {
        // $example on:create_ds$
        // Create an instance of a Bean class
        Person person = new Person();
        person.setName("Andy");
        person.setAge(32);

        // Encoders are created for Java beans
        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(Collections.singletonList(person), personEncoder);
        javaBeanDS.show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 32|Andy|
        // +---+----+

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1,
                integerEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on
        // name
        Dataset<Person> peopleDS = spark.read().json(jsonFilepath).as(personEncoder);
        peopleDS.show();
        // +----+-------+
        // | age| name|
        // +----+-------+
        // |null|Michael|
        // | 30| Andy|
        // | 19| Justin|
        // +----+-------+
        // $example off:create_ds$
        return peopleDS;
    }

    public static void main(String[] args) {
        try {
            // @formatter:off
            SparkSession spark = SparkSession.builder()
                    .appName("Java Spark SQL basic example")
                    .master("local[2]")
                    .config("spark.executor.memory", "1G")
                    .getOrCreate();
            // $example off:init_session$
            Dataset<Person> peopleDF = buildDF(spark, "src/main/resources/people.json");
            peopleDF.show();            
            peopleDF.createOrReplaceTempView("people");
            
            // SQL can be run over a temporary view created using DataFrames
            Dataset<Row> results = spark.sql("SELECT name FROM people");
 
            /*
             * The results of SQL queries are DataFrames and support all the normal RDD operations.
             * The columns of a row in the result can be accessed by field index or by field name
             */
            Dataset<String> namesDS = results.map((MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                    Encoders.STRING());
            namesDS.show();
            ArrayList<String> list = new ArrayList<>();     
            namesDS.foreach((v) -> {
                list.add(v);
            });
            System.out.println(list.toString());
            // SQL statements can be run by using the sql methods provided by spark
            // Dataset<Row> rows = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19");
            
            spark.stop();
         // @formatter:on
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }
}
