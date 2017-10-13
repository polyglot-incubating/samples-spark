package org.chiwooplatform.samples.support;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Java DSL 형태의 builder
 */
public class StructBuilder {

    private final List<StructField> fields;

    public StructBuilder() {
        fields = new ArrayList<>();
    }

    public FieldBuilder field(final String name) {
        return new FieldBuilder(this, name);
    }

    public StructType build() {
        assert this.fields.size() > 0 : "Required adding field one or more.";
        final StructType structType = DataTypes.createStructType(fields);
        return structType;
    }

    public static class FieldBuilder {
        final StructBuilder parent;
        private final String name;
        private DataType type = DataTypes.StringType;
        private boolean nullable = true;
        private Metadata meta = Metadata.empty();

        private FieldBuilder(StructBuilder builder, final String name) {
            this.parent = builder;
            this.name = name;
        }

        public FieldBuilder type(final DataType type) {
            this.type = type;
            return this;
        }

        public FieldBuilder nullable(final boolean nullable) {
            this.nullable = nullable;
            return this;
        }

        public FieldBuilder meta(final Metadata meta) {
            this.meta = meta;
            return this;
        }

        public StructBuilder add() {
            this.parent.fields.add(new StructField(this.name, this.type, this.nullable, this.meta));
            return this.parent;
        }
    }
}
