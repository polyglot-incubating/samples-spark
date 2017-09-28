package org.chiwooplatform.samples.ex01;

import org.apache.spark.api.java.JavaSparkContext;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.junit.Test;

public class Ex001SparkContext {

    @Test
    public void testContext() throws Exception {
        System.out.println("Create Spark Context...");
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex001SparkContext");
        System.out.println("spark.appName(): " + spark.appName());
        System.out.println("Close Spark Context...");
        spark.close();
    }

}
