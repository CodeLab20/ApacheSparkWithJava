package com.spark.sql.udf;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.Serializable;

public class TypeSafeAverageUDAF extends Aggregator<Employee, Average, Double> implements Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public Average zero() {
        return new Average(0,0);
    }

    @Override
    public Average reduce(Average buff, Employee e) {
        double newSum = buff.getSumVal() + e.getSal();
        long newCount = buff.getCountVal() + 1;
        buff.setCountVal(newCount);
        buff.setSumVal(newSum);
        return buff;
    }

    @Override
    public Average merge(Average b1, Average b2) {
        double newSum = b1.getSumVal() + b2.getSumVal();
        long newCount = b1.getCountVal() + b2.getCountVal();
        b1.setCountVal(newCount);
        b1.setSumVal(newSum);
        return b1;
    }

    @Override
    public Double finish(Average avg) {
        return avg.getSumVal()/avg.getCountVal();
    }

    @Override
    public Encoder<Average> bufferEncoder() {
        return Encoders.bean(Average.class);
    }

    @Override
    public Encoder<Double> outputEncoder() {
        return Encoders.DOUBLE();
    }

    @Override
    public String toString() {
        return "TypeSageAverageUDAF()";
    }
}
