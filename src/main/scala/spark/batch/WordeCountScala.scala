package spark.batch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by liuzhiwei on 2020/4/22
 *
 */
object WordeCountScala {

  //去掉不必要的日志
  Logger.getLogger("WordeCountScala").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)
    //1、读取文件，形成初始RDD，RDD是分为多个partition的，散落在集群的不同节点上，每个partition保存着文件的一部分信息，实现并行计算。
    val lines = sc.textFile("src/main/resources/data/wc.txt")
    //2、将每行拆分为单个单词
    val words = lines.flatMap(line => line.split(" "))
    //3、将每个单词映射为(word,1)的Tuple类型
    val pair = words.map(word => (word, 1))
    //4、将相同Key的value进行相加，实现单词计数功能
    val count = pair.reduceByKey((x, y) => x + y)
    count.foreach(println(_))
    //count.foreach(count => println(count._1 + " appears " + count._2 + " times."))

    /**
     * 1、2、3步是在同一节点上进行，不涉及跨节点传输数据，第4步会触发shuffle阶段，
     * 首先会在本地进行reduce，统计出本机的单词，之后执行shuffle阶段，
     * 将相同key的key-value对发送到同一分区，进行聚会，得出全局的单词统计数量。
     */

  }

}
