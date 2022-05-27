package connectivityWithPostgressSQL;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;

import java.util.HashMap;

import static org.apache.spark.sql.functions.*;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Created by Subhankar on 25-05-2022.
 */
public class ConnectionWithPostgressSql {

    public static void main(String[] args) {


        SparkSession spark = SparkSession
                .builder()
                .appName("ConnectionWithPostgressSql")
                .config("spark.master", "local")
                .getOrCreate();

        Dataset<Row> emp_df=spark.read()
                .format("jdbc")
                .option("url", "jdbc:postgresql://localhost:5432/postgres")
                .option("dbtable", "employee_schema.EMPLOYEE_DB")
                .option("user", "postgres")
                .option("password", "root")
                .load();

        emp_df.createOrReplaceTempView("emp");

        StructType schema = new StructType()
                .add("Plot No", StringType, true)
                .add("Street Number", StringType, true)
                .add("Block", StringType, true)
                .add("Pin", StringType, true);

        //Spark Convert JSON Column to struct Column
        Dataset<Row> df4=emp_df.withColumn("value",from_json(col("address"),schema));

        df4.show(false);
        df4.createOrReplaceTempView("emp_address");

        //Spark Convert JSON Column to Multiple Columns
        spark.sql("select *,concat_ws(',',phone_numbers) AS phone_numbers,value.* from emp_address").show(false);

        //Date Data type with case when statement
        spark.sql("SELECT *, CASE WHEN joining_date<='2021-01-01' then 'Eligible' ELSE 'Non Eligible' END AS Eligibility_For_Apprisal FROM emp_address").show(false);

    }
}
