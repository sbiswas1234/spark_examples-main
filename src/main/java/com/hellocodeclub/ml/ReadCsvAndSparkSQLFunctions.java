package com.hellocodeclub.ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;

import static org.apache.spark.sql.functions.*;


/**
 * Created by Subhankar on 13-05-2022.
 */
public class ReadCsvAndSparkSQLFunctions {
    public static void main(String[] args) {

      final String COLUMN_UPPERCASE_UDF_NAME = "columnUppercase";
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
        df3.groupBy("JOB_ID").sum("salary_to_Int").show(false);

       //orderBy ascending and descending
        df3.sort(col("salary_to_Int").desc()).show(false);
        df3.orderBy(col("salary_to_Int").asc()).show(false);


        //count
        df3.groupBy("JOB_ID").count().show();

        //max
        df3.groupBy("JOB_ID").max("salary_to_Int").show();

        // select distinct records for specific column
       df3.select("DEPARTMENT_ID").distinct().show(false);

       df3.dropDuplicates().show(false);

       df3.withColumn("Current_Date",current_date()).withColumn("Month Of the date",month(col("Current_Date"))).show();








    }
}
