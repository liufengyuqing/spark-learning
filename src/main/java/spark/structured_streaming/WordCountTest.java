package spark.structured_streaming;

import org.apache.spark.sql.SparkSession;

/**
 * create by liuzhiwei on 2020/6/14
 */
public class WordCountTest {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("WordCountTest").getOrCreate();
    }
}
