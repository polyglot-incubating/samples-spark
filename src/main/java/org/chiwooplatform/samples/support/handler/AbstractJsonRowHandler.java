package org.chiwooplatform.samples.support.handler;

import java.io.Serializable;

import org.apache.spark.sql.Row;

import com.fasterxml.jackson.databind.ObjectMapper;

@SuppressWarnings("serial")
public abstract class AbstractJsonRowHandler implements Serializable {

    final ObjectMapper mapper = new ObjectMapper();

    public Row execute(final String value) {
        try {
            final Row row = makeRow(mapper, value);
            return row;
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    abstract protected Row makeRow(final ObjectMapper mapper, final String value) throws Exception;

}
