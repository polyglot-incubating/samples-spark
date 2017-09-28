package org.chiwooplatform.samples.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import org.chiwooplatform.samples.support.handler.AbstractJsonRowHandler;

public class RddFunction {

    public static final MapFunction<Row, Row> rowByDelimiter(final String delimiter) {
    
        return (MapFunction<Row, Row>) row -> {
            final Object[] cols = row.getString(0).split(delimiter);
            return RowFactory.create(cols);
        };
    }

    @SuppressWarnings("serial")
    public static final FlatMapFunction<Row, Row> mergeCols = new FlatMapFunction<Row, Row>() {
    
        @Override
        public Iterator<Row> call(Row row) throws Exception {
            final Row newRow = RowFactory.create(row.mkString(":"));
            return Arrays.asList(newRow).iterator();
        }
    };

    public static final FlatMapFunction<Iterator<String>, Row> flatMapPartitions(final AbstractJsonRowHandler handler) {
    
        return new FlatMapFunction<Iterator<String>, Row>() {
            private static final long serialVersionUID = 1L;
    
            @Override
            public Iterator<Row> call(Iterator<String> lines) throws Exception {
                List<Row> rows = new ArrayList<Row>();
                while (lines.hasNext()) {
                    final String line = lines.next().trim();
                    if (line == null || line.length() < 3) {
                        continue;
                    }
                    try {
                        Row row = handler.execute(line);
                        if (row != null) {
                            rows.add(row);
                        }
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                return rows.iterator();
            }
        };
    }

}
