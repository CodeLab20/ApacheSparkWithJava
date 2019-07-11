package com.spark.files.txt;

import java.io.Serializable;

public class Person implements Serializable {
    private String name, job;
    private int age;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    /*
     * This method is necessary as when Person RDD is written to text file, toString() is called.
     */
    @Override
    public String toString() {
        return String.join(",", name, String.valueOf(age), job);
    }
}
