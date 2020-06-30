package spark.streaming;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * create by liuzhiwei on 2020/4/25
 */
public class SparkStreamingSocketTextStreamJava {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("SparkStreamingFileStreamJava").setMaster("local[2]");
        //conf.set("spark.testing.memory","2147480000");

        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(10));

        // Create a DStream that will connect to hostname:port, like localhost:9999
        JavaReceiverInputDStream<String> lines = javaStreamingContext.socketTextStream("localhost", 9999);

        // Split each line into words
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();
        //wordCounts.count();
        //wordCounts.dstream().saveAsTextFiles("hdfs://localhostt:9000/sparkResult/", "spark");
        //wordCounts.saveAsHadoopFiles("hdfs://localhostt:9000/sparkResult/", "spark", Text, IntWritable);

        javaStreamingContext.start();


        // Wait for the computation to terminate
        javaStreamingContext.awaitTermination();

    }
}
