package org.chiwooplatform.samples.support;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public final class SparkContextHolder {

    public static JavaSparkContext getLocalContext(final String appName) {
        // @formatter:off
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName(appName)
                .set("spark.executor.memory",
                "512M");
        return new JavaSparkContext(conf);
        // @formatter:on
    }

    public static SparkSession getLocalSession(final String appName) {
        // @formatter:off
        final SparkSession spark = SparkSession.builder()
                .appName(appName)
                .master("local[2]")
                .config("spark.executor.memory", "512M")
                .getOrCreate();
        return spark;
        // @formatter:on
    }
}
