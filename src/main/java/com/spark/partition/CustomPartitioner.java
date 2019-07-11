package com.spark.partition;

import org.apache.spark.Partitioner;

public class CustomPartitioner extends Partitioner {

    private int maxPartitions = 2;

    public CustomPartitioner(){}

    public CustomPartitioner(int maxPartitions)
    {
        this.maxPartitions = maxPartitions;
    }

    @Override
    public int numPartitions() {
        return maxPartitions;
    }

    @Override
    public int getPartition(Object key) {
        return String.valueOf(key).length() % maxPartitions;
    }
}
