package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * create by liuzhiwei on 2020/5/9
 */
public class SparkSQLJava {
    public static void main(String[] args) {
        // 方式一、通过SparkSession构建
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkSQLJava")
                .master("local[2]")
                // 如果要读取Hive中的表，必须使用这个
                // 如果确定不使用Hive中的数据，建议不用开启，启动的时候会去连接Hive
                //.enableHiveSupport()
                .getOrCreate();

        // 方式二、通过SparkConf构建
        // 建议通过方式二构建，SparkConf不仅Spark SQL可以使用，其他的Spark模块也可以使用
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkSQLJava")
                .setMaster("local[2]");

        SparkSession spark2 = SparkSession
                .builder()
                .config(sparkConf)
                // 如果要读取Hive中的表，必须使用这个
                // 如果确定不使用Hive中的数据，建议不用开启，启动的时候会去连接Hive
                //.enableHiveSupport()
                .getOrCreate();

        Dataset<Row> df = spark.read().json("spark-learning/data/people.json");
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(col("name"), col("age").plus(1)).show();
        df.filter(col("age").gt(21)).show();
        df.groupBy("age").count().show();

        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("select * from people");
        sqlDF.show();
    }
}
