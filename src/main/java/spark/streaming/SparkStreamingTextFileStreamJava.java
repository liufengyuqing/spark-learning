package spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * create by liuzhiwei on 2020/4/26
 */
public class SparkStreamingTextFileStreamJava {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingTextFileStreamJava");

        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));

        JavaDStream<String> lines = jsc.textFileStream("spark-learning/data");

        JavaDStream<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));

        JavaPairDStream<String, Integer> wordCouns = pairs.reduceByKey((v1, v2) -> v1 + v2);

        wordCouns.print();

        jsc.start();
        jsc.awaitTermination();
    }

}
