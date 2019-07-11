package com.spark.sql.udf;

import java.io.Serializable;

public class Average implements Serializable {

    private static final long serialVersionUID = 1L;
    private double sumVal;
    private long countVal;

    public Average() {
    }

    public Average(double sumVal, long countVal) {
        this.sumVal = sumVal;
        this.countVal = countVal;
    }

    public double getSumVal() {
        return sumVal;
    }

    public void setSumVal(double sumVal) {
        this.sumVal = sumVal;
    }

    public long getCountVal() {
        return countVal;
    }

    public void setCountVal(long countVal) {
        this.countVal = countVal;
    }
}
