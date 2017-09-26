package org.chiwooplatform.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public final class SparkUtils {

    public static Column column(final String columnName) {
        return org.apache.spark.sql.functions.col(columnName);
    }

    private static final BiFunction<String, DataType, StructField> buildStructField = (name, type) -> DataTypes
            .createStructField(name, type, true);

    public static StructType stringSchema(final String columnName) {
        return SparkUtils.buildSchema(new String[] { columnName }, new DataType[] { DataTypes.StringType });
    }

    public static StructType buildSchema(final String[] tuples, final DataType[] types) {
        List<StructField> fields = new ArrayList<>();
        IntStream.range(0, tuples.length).mapToObj(i -> buildStructField.apply(tuples[i], types[i])).forEach(f -> {
            // System.out.printf("\n %s : %s : %s", f.name(), f.dataType(),
            // f.getClass().getName());
            fields.add(f);
        });
        final StructType structType = DataTypes.createStructType(fields);
        return structType;
    }

    public static final void log(List<? extends Object> data) {
        for (Object value : data) {
            if (value instanceof Tuple2) {
                Tuple2<?, ?> tuple = (Tuple2<?, ?>) value;
                System.out.printf("%s: %s  \n", tuple._1(), tuple._2());
            }
            else {
                System.out.println(value);
            }
        }
    }

    // public static final void logPairInt(final List<Tuple2<String, Integer>> tuples) {
    // for (Tuple2<String, ? extends Number> tuple : tuples) {
    // System.out.printf("%s: %s \n", tuple._1(), tuple._2());
    // }
    // }

}
