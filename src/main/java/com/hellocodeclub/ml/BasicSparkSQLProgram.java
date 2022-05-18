package com.hellocodeclub.ml;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.col;


/**
 * Created by Subhankar on 12-05-2022.
 */
public class BasicSparkSQLProgram {
    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master", "local")
                .getOrCreate();


        Dataset<Row> df = spark.read().json("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\people.json");


        df.show();


        // col("...") is preferable to df.col("...")


// Print the schema in a tree format
        df.printSchema();


// root
// |-- age: long (nullable = true)
// |-- name: string (nullable = true)

// Select only the "name" column
        df.select("name").show();
// +-------+
// |   name|
// +-------+
// |Michael|
// |   Andy|
// | Justin|
// +-------+

// Select everybody, but increment the age by 1
        df.select(col("name"), col("age").plus(1)).show();
// +-------+---------+
// |   name|(age + 1)|
// +-------+---------+
// |Michael|     null|
// |   Andy|       31|
// | Justin|       20|
// +-------+---------+

// Select people older than 21
        df.filter(col("age").gt(21)).show();
// +---+----+
// |age|name|
// +---+----+
// | 30|Andy|
// +---+----+

// Count people by age
        df.groupBy("age").count().show();

// +----+-----+
// | age|count|
// +----+-----+
// |  19|    1|
// |null|    1|
// |  30|    1|
// +----+-----+



   WindowSpec ws= Window.partitionBy("department").orderBy("salary");

   //Row_Number()
        df.withColumn("row_number",functions.row_number().over(ws))
                .show();

        df.withColumn("rank_people",functions.rank().over(ws))
                .show();

     //Dense_Rank()
        df.withColumn("dense_rank_people",functions.dense_rank().over(ws))
                .show();


       //lag
        df.withColumn("lag",functions.lag("salary",1).over(ws))
                .show();

        //lead
        df.withColumn("lead",functions.lead("salary",1).over(ws))
                .show();

    }
}
