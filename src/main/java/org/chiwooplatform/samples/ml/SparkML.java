package org.chiwooplatform.samples.ml;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.chiwooplatform.samples.support.StructBuilder;
import org.junit.Test;

public class SparkML {

    @Test
    public void testStructBuilder() throws Exception {
        StructType schema = new StructBuilder()
                .field("id").nullable(false).add()
                .field("name").nullable(false).add()
                .field("description").add()
                .field("age").type(DataTypes.IntegerType).add()
                .build();
        System.out.println("schema: " + schema);
    }
}
