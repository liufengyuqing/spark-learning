package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * create by liuzhiwei on 2020/4/25
 *
 */
object SparkStreamingTextFileStreamScala {
  def main(args: Array[String]): Unit = {
    //设置本地运行模式，两个线程，一个监听，另一个处理数据
    val conf = new SparkConf().setAppName("SparkStreamingDemoFileStreamScala").setMaster("local[2]")
    //时间间隔为2
    //创建StreamingContext需要两个参数：SparkConf和batch interval
    val streamingContext = new StreamingContext(conf, Seconds(10))
    //文件流 监听文件夹下新文件的生成
    val lines = streamingContext.textFileStream("file:///Users/liuzhiwei/IdeaProjects/BigDataProject/spark-learning/data")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    //启动流计算过程
    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
