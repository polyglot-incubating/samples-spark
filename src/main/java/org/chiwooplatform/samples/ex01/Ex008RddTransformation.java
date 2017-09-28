package org.chiwooplatform.samples.ex01;

import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Ex008RddTransformation {

    private static JavaSparkContext ctx;

    @BeforeClass
    static public void setUp() {
        ctx = SparkContextHolder.getLocalContext("RDDTransformations");
    }

    @AfterClass
    static public void dispose() {
        try {
            ctx.close();
        }
        catch (Exception e) {
        }

    }

    @Test
    public void ut1001_buildRDD() throws Exception {
        JavaRDD<String> rdd = ctx
                .parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();
        log.info("\n\n\n{}\n\n\n", rdd.collect().toString());
    }

    @Test
    public void ut1002_filter() throws Exception {
        JavaRDD<String> rdd = ctx
                .parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();

        log.info("\n\n HAS-STARTED-JOB-FOR-LOADING-DATA  \n\n");
        // Filter
        JavaRDD<String> filteredRDD = rdd.filter(x -> x.startsWith("L"));
        log.info("\n\n\n{}\n\n\n", filteredRDD.collect());
    }

    @Test
    public void ut1003_map_toUppercase() throws Exception {
        JavaRDD<String> rdd = ctx
                .parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();

        log.info("\n\n HAS-STARTED-JOB-FOR-LOADING-DATA  \n\n");
        // Filter
        JavaRDD<String> mapedRDD = rdd.map(x -> x.toUpperCase());
        log.info("\n\n\n{}\n\n\n", mapedRDD.collect());
    }

    @Test
    public void ut1004_distinct() throws Exception {
        JavaRDD<String> rdd = ctx
                .parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();

        log.info("\n\n HAS-STARTED-JOB-FOR-LOADING-DATA  \n\n");
        // Filter
        JavaRDD<String> mapedRDD = rdd.distinct();
        log.info("\n\n\n{}\n\n\n", mapedRDD.collect());
    }

    @Test
    public void ut1005_flatMap_buildNewRDD() throws Exception {
        JavaRDD<String> rdd = ctx
                .parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();

        log.info("\n\n HAS-STARTED-JOB-FOR-LOADING-DATA  \n\n");
        // Filter
        JavaRDD<String> flatMapedRDD = rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        log.info("\n\n\n{}\n\n\n", flatMapedRDD.collect());
    }

    @Test
    public void ut1006_union() throws Exception {
        JavaRDD<String> rdd = ctx
                .parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();
        JavaRDD<String> rdd2 = ctx.parallelize(Arrays.asList("Han Solo", "Lando Calrissian")).cache();

        log.info("\n\n HAS-STARTED-JOB-FOR-LOADING-DATA  \n\n");
        // Filter
        JavaRDD<String> unionRDD = rdd.union(rdd2);
        log.info("\n\n\n{}\n\n\n", unionRDD.collect());
    }

    @Test
    public void ut1007_subtract() throws Exception {
        JavaRDD<String> rdd = ctx
                .parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();
        JavaRDD<String> rdd2 = ctx.parallelize(Arrays.asList("Han Solo", "Lando Calrissian")).cache();

        log.info("\n\n HAS-STARTED-JOB-FOR-LOADING-DATA  \n\n");
        // Filter
        JavaRDD<String> subtractRDD = rdd.subtract(rdd2);
        log.info("\n\n\n{}\n\n\n", subtractRDD.collect());
    }

    @Test
    public void ut1008_intersection() throws Exception {
        JavaRDD<String> rdd = ctx
                .parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();
        JavaRDD<String> rdd2 = ctx.parallelize(Arrays.asList("Han Solo", "Lando Calrissian")).cache();

        log.info("\n\n HAS-STARTED-JOB-FOR-LOADING-DATA  \n\n");
        // Filter
        JavaRDD<String> intersectionRDD = rdd.intersection(rdd2);
        log.info("\n\n\n{}\n\n\n", intersectionRDD.collect());
    }

    @Test
    public void ut1009_cartesian() throws Exception {
        JavaRDD<String> rdd = ctx
                .parallelize(Arrays.asList("Luke Skywalker", "Leia Organa", "Han Solo", "Luke Skywalker")).cache();
        JavaRDD<String> rdd2 = ctx.parallelize(Arrays.asList("Han Solo", "Lando Calrissian")).cache();

        log.info("\n\n HAS-STARTED-JOB-FOR-LOADING-DATA  \n\n");
        // Filter
        JavaPairRDD<String, String> resultRDD = rdd.cartesian(rdd2);
        log.info("\n\n\n{}\n\n\n", resultRDD.collect());
    }

}
