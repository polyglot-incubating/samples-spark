package org.chiwooplatform.samples.ex02;

import java.util.Arrays;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.junit.Test;

/**
 * scala> data.aggregate((0,0))((x,y)=>(x._1+y,x._2+1),(x,y)=>(x._1+y._1,x._2+y._2))
 */
@SuppressWarnings("serial")
public class Ex204AvgCount implements Serializable {

    static class AvgCount implements Serializable {
        public int total;
        public int num;

        public AvgCount(int total, int num) {
            this.total = total;
            this.num = num;
        }

        public double avg() {
            return total / (double) num;
        }
    }

    private Function2<AvgCount, Integer, AvgCount> addAndCount() {
        return new Function2<AvgCount, Integer, AvgCount>() {
            public AvgCount call(AvgCount a, Integer x) {
                a.total += x;
                a.num += 1;
                return a;
            }
        };
    }

    private Function2<AvgCount, AvgCount, AvgCount> combine() {
        return new Function2<AvgCount, AvgCount, AvgCount>() {
            public AvgCount call(AvgCount a, AvgCount b) {
                a.total += b.total;
                a.num += b.num;
                return a;
            }
        };
    }

    /**
     * 평균을 계산
     */
    @Test
    public void testRDD() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex201AggregationAvg");
        JavaRDD<Integer> rdd = spark.parallelize(Arrays.asList(1, 2, 3, 4));
        Function2<AvgCount, Integer, AvgCount> addAndCount = addAndCount();
        Function2<AvgCount, AvgCount, AvgCount> combine = combine();
        AvgCount initial = new AvgCount(0, 0);
        AvgCount result = rdd.aggregate(initial, addAndCount, combine);
        System.out.println("result.avg(): " + result.avg());
        spark.close();
    }

}
