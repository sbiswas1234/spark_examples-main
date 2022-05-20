package com.hellocodeclub.ml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.add_months;
import static org.apache.spark.sql.functions.col;

/**
 * Created by Subhankar on 13-05-2022.
 */
public class ReadOrcFileAndRepartitionUsingSpark {
    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("ReadParquetAndSparkSQL")
                .config("spark.master", "local")
                .getOrCreate();



        Dataset<Row> orcDf = spark.read()
                .orc("C:\\Users\\Subhankar\\Documents\\Dataset\\spark_examples-main\\src\\orc-file-11-format.orc");

        orcDf.show();

        orcDf.printSchema();

        System.out.println(orcDf.rdd().getNumPartitions());

        orcDf.repartition(5);

        orcDf.coalesce(3);




        Dataset<Row> orcDfPersist = orcDf.persist(StorageLevel.DISK_ONLY());


        orcDfPersist.show(false);


    }
}
