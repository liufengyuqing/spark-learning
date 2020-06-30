package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Collections;

/**
 * create by liuzhiwei on 2020/5/10
 */
public class SparkSQLDataSetJava {
    public static void main(String[] args) throws AnalysisException {
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkSQLDataSetJava")
                .setMaster("local[2]");

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

        Dataset<Row> df = spark.read().json("spark-learning/data/people.json");

        df.createGlobalTempView("people");

        spark.sql("select * from global_temp.people").show();

        spark.newSession().sql("select * from global_temp.people").show();

        People people = new People();
        people.setName("Andy");
        people.setAge(32);

        Encoder<People> peopleEncoder = Encoders.bean(People.class);
        Dataset<People> javaBeanDS = spark.createDataset(Collections.singletonList(people), peopleEncoder);
        javaBeanDS.show();


        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);

        Dataset<Integer> map = primitiveDS.map((MapFunction<Integer, Integer>) value -> value + 1, integerEncoder);
        map.collect();


        Dataset<People> peopleDataset = df.as(peopleEncoder);
        peopleDataset.show();

    }
}
