package org.chiwooplatform.samples.ex01;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.chiwooplatform.samples.support.SparkContextHolder;
import org.chiwooplatform.samples.support.SparkUtils;
import org.junit.Test;

public class Ex005UserFunction {

    static class ContainsDBus implements Function<String, Boolean> {
        private static final long serialVersionUID = 2031345049069606346L;

        public Boolean call(String x) {
            return x.contains("dbus");
        }
    }

    @SuppressWarnings("serial")
    static private Function<String, Boolean> constains(final String value) {
        return new Function<String, Boolean>() {
            public Boolean call(String x) {
                return x.contains(value);
            }
        };
    }

    /**
     * 사용자 정의 함수 예제
     * @throws Exception
     */
    @Test
    public void testRDD() throws Exception {
        JavaSparkContext spark = SparkContextHolder.getLocalContext("Ex005UserFunction");
        JavaRDD<String> rdd = spark.textFile("src/main/resources/log.txt").filter(new ContainsDBus());
        SparkUtils.log(rdd.collect());

        JavaRDD<String> rdd2 = spark.textFile("src/main/resources/log.txt").filter(constains("avahi"));
        SparkUtils.log(rdd2.collect());
        spark.close();
    }

}
