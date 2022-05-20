package com.hellocodeclub.ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

/**
 * Created by Subhankar on 13-05-2022.
 */
public class AllTypeSparkSQLJoins {
    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("ReadCsvAndSparkSQLFunctions")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> df3 = spark.read().option("header", "true").csv("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\employees.csv");


        df3.show(false);


        Dataset<Row> csvFileDF2 = spark.read().option("header", "true").csv("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\depatment.csv");

        csvFileDF2.show();


        //joining...DSL approach

        //inner join
        Dataset <Row> joined = df3.join(csvFileDF2, df3.col("DEPARTMENT_ID").equalTo(csvFileDF2.col("DEPARTMENT_ID")),"inner");

        joined.show(false);

        //leftouter join

        df3.join(csvFileDF2, df3.col("DEPARTMENT_ID").equalTo(csvFileDF2.col("DEPARTMENT_ID")),"leftouter").show(false);

        //rightouter join
        df3.join(csvFileDF2, df3.col("DEPARTMENT_ID").equalTo(csvFileDF2.col("DEPARTMENT_ID")),"rightouter").show(false);

        //fullouter join
        df3.join(csvFileDF2, df3.col("DEPARTMENT_ID").equalTo(csvFileDF2.col("DEPARTMENT_ID")),"fullouter").show(false);

        //leftsemi join
        df3.join(csvFileDF2, df3.col("DEPARTMENT_ID").equalTo(csvFileDF2.col("DEPARTMENT_ID")),"leftsemi").show(false);

        //leftanti join
        df3.join(csvFileDF2, df3.col("DEPARTMENT_ID").equalTo(csvFileDF2.col("DEPARTMENT_ID")),"leftanti").show(false);

        //self join
        df3.as("emp1").join(df3.as("emp2"), df3.col("DEPARTMENT_ID").equalTo(df3.col("DEPARTMENT_ID")),"inner").show(false);

        //SQL JOIN(sql approach)
        df3.createOrReplaceTempView("EMP");
        csvFileDF2.createOrReplaceTempView("DEPT");

        Dataset<Row> joinDF = spark.sql("select * from EMP e, DEPT d where e.DEPARTMENT_ID == d.DEPARTMENT_ID");
        joinDF.show(false);

        Dataset<Row> joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.DEPARTMENT_ID == d.DEPARTMENT_ID");
        joinDF2.show(false);

        joinDF2.cache();

    }
}
