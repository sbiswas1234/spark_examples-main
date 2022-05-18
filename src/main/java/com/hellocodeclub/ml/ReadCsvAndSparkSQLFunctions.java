package com.hellocodeclub.ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.IntegerType;

import static org.apache.spark.sql.functions.add_months;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;
/**
 * Created by Subhankar on 13-05-2022.
 */
public class ReadCsvAndSparkSQLFunctions {
    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("ReadCsvAndSparkSQLFunctions")
                .config("spark.master", "local")
                .getOrCreate();

       // Dataset<Row> peopleDF = spark.read().json("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\people.json");

// DataFrames can be saved as Parquet files, maintaining the schema information
        //peopleDF.write().parquet("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\people.parquet");

        // Read in the Parquet file created above.
// Parquet files are self-describing so the schema is preserved
// The result of loading a parquet file is also a DataFrame
        Dataset<Row> csvFileDF = spark.read().option("header", "true").csv("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\employees.csv");


        csvFileDF.show(false);
       //case when statement in spark sql
        csvFileDF = csvFileDF.withColumn("Grade_Of_Employee", when(col("SALARY").$greater(10000),"Grade 3")
                .when((col("SALARY").$less$eq(10000)).and(col("SALARY").$greater$eq(6000)) ,"Grade 2")
                .when(col("SALARY").$less(6000) ,"Grade 1")
                .otherwise("Contract"));

        csvFileDF.show(false);

        //Renaming column names
        Dataset<Row> df2 = csvFileDF.withColumnRenamed("EMPLOYEE_ID","EMPLOYEE_NO")
                .withColumnRenamed("salary","salary_amount");
        df2.printSchema();

        //Changing data type of column

        Dataset<Row> df3=df2.withColumn("salary_to_Int",col("salary_amount").cast("int"));

        df3.show(false);

        df3.printSchema();

       // groupBy and sum()
        df3.groupBy("JOB_ID").avg("salary_to_Int").show(false);

       //orderBy ascending and descending
        df3.sort(col("salary_to_Int").desc()).show(false);
        df3.orderBy(col("salary_to_Int").asc()).show(false);


        //count
        df3.groupBy("JOB_ID").count().show();

        //max
        df3.groupBy("JOB_ID").max("salary_to_Int").show();

        // select distinct records for specific column
       df3.select("DEPARTMENT_ID").distinct().show(false);

       // df3.dropDuplicates().show(false);

        Dataset<Row> csvFileDF2 = spark.read().option("header", "true").csv("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\depatment.csv");

        csvFileDF2.show();


        //joining...DSL approach

        //inner join
        Dataset <Row> joined = df3.join(csvFileDF2, df3.col("DEPARTMENT_ID").equalTo(csvFileDF2.col("DEPARTMENT_ID")));

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

    }
}
