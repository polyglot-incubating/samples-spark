package org.chiwooplatform.samples.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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

    @SuppressWarnings("serial")
    public static final FlatMapFunction<String, String> toStringByNewLine = new FlatMapFunction<String, String>() {
        private final Pattern NL = Pattern.compile("\\n");

        @Override
        public Iterator<String> call(String value) throws Exception {
            String[] cols = NL.split(value);
            return Arrays.asList(cols).iterator();
        }
    };

    @SuppressWarnings("serial")
    public static final FlatMapFunction<String, String> toRowsByNewLine = new FlatMapFunction<String, String>() {
        private final Pattern NL = Pattern.compile("\\n");

        @Override
        public Iterator<String> call(final String value) throws Exception {
            String[] rows = NL.split(value);
            final List<String> list = new LinkedList<>();
            for (String row : rows) {
                if (row != null && row.trim().length() > 0) {
                    list.add(row);
                }
            }
            return list.iterator();
        }
    };

    @SuppressWarnings("serial")
    public static final FlatMapFunction<String, Row> toRowByDelimiter(final String delimiter) {
        return new FlatMapFunction<String, Row>() {
            private final Pattern DELIMITER = Pattern.compile(delimiter);

            @Override
            public Iterator<Row> call(final String value) throws Exception {
                String[] rows = DELIMITER.split(value);
                final List<Row> list = new LinkedList<>();
                for (String row : rows) {
                    list.add(RowFactory.create(row));
                }
                return list.iterator();
            }
        };
    }

    @SuppressWarnings("serial")
    public static final Function<String, Row> makeRowByDelimiter(final String delimiter) {
        return new Function<String, Row>() {
            private final Pattern DELIMITER = Pattern.compile(delimiter);

            @Override
            public Row call(final String value) throws Exception {
                Object[] rows = DELIMITER.split(value);
                return RowFactory.create(rows);
            }
        };
    }

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
