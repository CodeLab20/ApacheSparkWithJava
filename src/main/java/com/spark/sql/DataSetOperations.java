package com.spark.sql;

import com.spark.files.csv.Movie;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class DataSetOperations {
    public static void main(String[] args) {
        SparkSession ss =new SparkSession.Builder()
                            .master("local[1]").appName("DataSetOperations")
                            .config("spark.hadoop.validateOutputSpecs", "false")
                            .enableHiveSupport()    //For querying using hive queries
                            .getOrCreate();
        JavaSparkContext jsc  = new JavaSparkContext(ss.sparkContext());

        JavaRDD<String> rdd = jsc.textFile("InputFiles/departments.csv");
        JavaRDD<String> hdrlessRdd = rdd.filter(s -> !s.startsWith("deptno")); //remove header


        //using RowFactory to create rows and creating custom schema
        JavaRDD<Row> mvRows = hdrlessRdd.map(s -> {
            String[] split = s.split(",");
            return RowFactory.create(Integer.parseInt(split[0]), split[1], split[2]);
        });

        //During custom schema creation, column names are changed.
        List<StructField> fields = new ArrayList<>();
        fields.add(DataTypes.createStructField("DeptNo", DataTypes.IntegerType, false));
        fields.add(DataTypes.createStructField("DeptName", DataTypes.StringType, false));
        fields.add(DataTypes.createStructField("DeptLoc", DataTypes.StringType, false));

        StructType schema = DataTypes.createStructType(fields);

        System.out.println("Departments created using Custom schema:::");

        Dataset<Row> deptDf = ss.createDataFrame(mvRows, schema);
        deptDf.printSchema();
        deptDf.show();

        //Custom SQL Query with Temporary View from DataSet
        System.out.println("Custom SQL Query for DeptLocation and its count");
        deptDf.createOrReplaceTempView("DEPT");

        Dataset<Row> result = ss.sql("select DeptLoc, count(DeptLoc) from DEPT where DeptNo > 10 group by DeptLoc");
        result.show();

        //Custom SQL Query with Global Temporary View from DataSet
        System.out.println("Custom SQL Query on Global Temp View");
        deptDf.createOrReplaceGlobalTempView("DeptGlobal");
        Dataset<Row> globalRes = ss.newSession().sql("select DeptNo, DeptName, DeptLoc, rank() OVER (PARTITION BY DeptLoc order by DeptNo) as rank " +
                " from global_temp.DeptGlobal");
        globalRes.show();

        //Saving Dataset to disk
        System.out.println("Json Output::");
        deptDf.write().mode(SaveMode.Overwrite).json("DataSetOutput/dept_json");

        System.out.println("CSV Output::");
        deptDf.write().mode(SaveMode.Overwrite).csv("DataSetOutput/dept_csv");

        System.out.println("Default Output in Parquet format::");
        deptDf.write().mode(SaveMode.Overwrite).save("DataSetOutput/dept_default");

        System.out.println("Save as Table::");
        deptDf.write().mode(SaveMode.Overwrite).format("csv").saveAsTable("Department");

        //Don't know where this stores file
//        System.out.println("Save as Table with Path::");
//        deptDf.write().mode(SaveMode.Overwrite).format("csv")
//                .option("path", "file:///DataSetOutput/hive").saveAsTable("Department");

        //Read Employee Data
        System.out.println("Read Employee Data:::");
        Dataset<Row> empDs = ss.read().format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("InputFiles/employees.csv");

        empDs.printSchema();
        empDs.show();

        System.out.println("Dataset Projections::");
        empDs.select("ename", "job").show();

        System.out.println("Dataset Projections with alias::");
        empDs.select(
                col("ename").as("EmpName"),     //using as function for alias
                col("job").name("EmpPost"),     //using name function for alias
                col("empno").as("EmpNo").cast(DataTypes.IntegerType)
                ).show();

        System.out.println("Dataset Sort with filter");
        empDs.sort(col("empno").asc()).filter(col("sal").gt("2500")).show();

        System.out.println("Dataset group by job with job counts");
        empDs.select("job").groupBy(col("job")).count().show();


        //join operations
        System.out.println("Left Join of  Employees Dataset with Departments Dataset");
        empDs.as("emp")
                .join(deptDf.as("dept"),
                      empDs.col("deptno").equalTo(deptDf.col("DeptNo")),
                        "left"
                ).select("emp.empno",  "dept.DeptNo", "emp.ename", "dept.DeptName", "emp.sal")
                .show();

        System.out.println("Right Join of  Employees Dataset with Departments Dataset");
        empDs.as("emp")
                .join(deptDf.as("dept"),
                        empDs.col("deptno").equalTo(deptDf.col("DeptNo")),
                        "right"
                ).select("emp.empno",  "dept.DeptNo", "emp.ename", "dept.DeptName", "emp.sal")
                .show();

        System.out.println("Logical Plan for Right Join of Employees Dataset with Departments Dataset");
        empDs.as("emp")
                .join(deptDf.as("dept"),
                        empDs.col("deptno").equalTo(deptDf.col("DeptNo")),
                        "right"
                ).logicalPlan();

        System.out.println("Explain Plan for Right Join of Employees Dataset with Departments Dataset");
        empDs.as("emp")
                .join(deptDf.as("dept"),
                        empDs.col("deptno").equalTo(deptDf.col("DeptNo")),
                        "right"
                ).explain();


        jsc.close();
        ss.close();
        System.out.println("Done!!!");
    }

}
