package org.chiwooplatform.samples;

import org.apache.spark.api.java.JavaSparkContext;

import org.chiwooplatform.samples.support.SparkContextHolder;

/**
 * Spark 컨텍스트를 열로 닫는 것부터가 시작이다.
 */
public class SparkMain {

    public static void main(String[] args) {
        JavaSparkContext ctx = SparkContextHolder.getLocalContext("SparkApp");
        System.out.println("appName: " + ctx.appName());
        ctx.close();
    }
}
