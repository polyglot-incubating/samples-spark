package org.chiwooplatform.samples.rdd;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FileDataFrame {

    private static void buildDF(final SparkSession spark, final String jsonFilepath) throws AnalysisException {
        // $example on:create_df$
        Dataset<Row> df = spark.read().json(jsonFilepath);

        // Displays the content of the DataFrame to stdout
        df.show();
        // +----+-------+
        // | age| name|
        // +----+-------+
        // |null|Michael|
        // | 30| Andy|
        // | 19| Justin|
        // +----+-------+
        // $example off:create_df$

        // $example on:untyped_ops$
        // Print the schema in a tree format
        df.printSchema();
        // root
        // |-- age: long (nullable = true)
        // |-- name: string (nullable = true)

        // Select only the "name" column
        df.select("name").show();
        // +-------+
        // | name|
        // +-------+
        // |Michael|
        // | Andy|
        // | Justin|
        // +-------+

        // Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();
        // +-------+---------+
        // | name|(age + 1)|
        // +-------+---------+
        // |Michael| null|
        // | Andy| 31|
        // | Justin| 20|
        // +-------+---------+

        // Select people older than 21
        df.filter(col("age").gt(21)).show();
        // +---+----+
        // |age|name|
        // +---+----+
        // | 30|Andy|
        // +---+----+

        // Count people by age
        df.groupBy("age").count().show();
        // +----+-----+
        // | age|count|
        // +----+-----+
        // | 19| 1|
        // |null| 1|
        // | 30| 1|
        // +----+-----+
        // $example off:untyped_ops$

        // $example on:run_sql$
        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
        sqlDF.show();
        // +----+-------+
        // | age| name|
        // +----+-------+
        // |null|Michael|
        // | 30| Andy|
        // | 19| Justin|
        // +----+-------+
        // $example off:run_sql$

        // $example on:global_temp_view$
        // Register the DataFrame as a global temporary view
        df.createGlobalTempView("people");

        // Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age| name|
        // +----+-------+
        // |null|Michael|
        // | 30| Andy|
        // | 19| Justin|
        // +----+-------+

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
        // +----+-------+
        // | age| name|
        // +----+-------+
        // |null|Michael|
        // | 30| Andy|
        // | 19| Justin|
        // +----+-------+
        // $example off:global_temp_view$
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
            buildDF(spark, "src/main/resources/people.json");
            spark.stop();
         // @formatter:on
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
