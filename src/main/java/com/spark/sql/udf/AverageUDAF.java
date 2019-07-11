package com.spark.sql.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.*;

import javax.xml.crypto.Data;

/**
 * User Defined Aggregate Function
 */

public class AverageUDAF extends UserDefinedAggregateFunction {

    // This method describes the schema of input to the UDAF. Spark UDAFs can be
    // defined to operate on any number of columns. Since, we are implementing the
    // the UDAF for average which require only one value as input.

    @Override
    public StructType inputSchema() {
        return new StructType(new StructField[]{
                new StructField("value", DataTypes.DoubleType, true, Metadata.empty())
        });
    }

    // Similar to inputSchema, this method describes the schema of UDAF buffer.
    // For average, we need to maintain two pieces of information,
    // one is the count of values other is the sum of values.
    @Override
    public StructType bufferSchema() {
        return new StructType()
                .add(new StructField("sumVal", DataTypes.DoubleType, true, Metadata.empty()))
                .add(new StructField("countVal", DataTypes.LongType, true, Metadata.empty()));
    }

    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    // This describes whether the UDAF we are implementing is deterministic or not.
    // Since, spark executes by splitting data, processing the chunks separately and combining them.
    // If the UDAF logic is such that the result is independent of the order in which data is
    // processed and combined then the UDAF is deterministic.
    @Override
    public boolean deterministic() {
        return true;
    }

    // This method is used to initialize the buffer. This method can be
    // called any number of times of spark during processing.
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0.0);
        buffer.update(1, 0L);
    }

    // This method takes a buffer and an input row and updates the
    // buffer. Any validations on input can be performed in this method.
    @Override
    public void update(MutableAggregationBuffer buffer, Row row) {
        buffer.update(0, buffer.getDouble(0) + row.getDouble(0));
        buffer.update(1, buffer.getLong(1) + 2);
    }

    // This method takes two buffers and combines them to produce a
    // single buffer.
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row row) {
        buffer1.update(0, buffer1.getDouble(0) + row.getDouble(0));
        buffer1.update(1, buffer1.getLong(1) + row.getLong(1));
    }

    // This method will be called when all processing is complete and
    // there is only one buffer left. This will return the final value
    // of UDAF.
    @Override
    public Object evaluate(Row row) {
        return row.getDouble(0) / row.getLong(1);
    }
}
