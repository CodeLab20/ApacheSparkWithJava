package com.spark.shared;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ListAccumulator extends AccumulatorV2<String, CopyOnWriteArrayList<Integer>> {

    private CopyOnWriteArrayList<Integer> lst = null;

    public ListAccumulator()
    {
        lst = new CopyOnWriteArrayList<>();
    }

    public ListAccumulator(CopyOnWriteArrayList<Integer> val)
    {
        if(val != null && ! val.isEmpty())
            lst = new CopyOnWriteArrayList<>(val);
    }

    @Override
    public boolean isZero() {
        return lst.isEmpty();
    }

    @Override
    public AccumulatorV2<String, CopyOnWriteArrayList<Integer>> copy() {
        return new ListAccumulator();
    }

    @Override
    public void reset() {
        lst = new CopyOnWriteArrayList<>();
    }

    @Override
    public void add(String v) {
        lst.add(Integer.parseInt(v));
    }

    @Override
    public void merge(AccumulatorV2<String, CopyOnWriteArrayList<Integer>> other) {
       value().addAll(other.value());
    }

    @Override
    public CopyOnWriteArrayList<Integer> value() {
        return lst;
    }
}
