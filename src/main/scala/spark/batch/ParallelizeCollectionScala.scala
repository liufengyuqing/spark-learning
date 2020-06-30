package spark.batch

import org.apache.spark.{SparkConf, SparkContext}

/**
 * create by liuzhiwei on 2020/4/22
 *
 */
object ParallelizeCollectionScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val rdd = sc.parallelize(array)

    val sum = rdd.reduce((x, y) => x + y)

    println(sum)
  }

}
