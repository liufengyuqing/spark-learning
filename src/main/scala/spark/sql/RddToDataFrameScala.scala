package spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * create by liuzhiwei on 2020/5/12
 *
 */
object RddToDataFrameScala {

  case class Dept(deptId: Int, deptName: String, deptAddr: String)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("RddToDataFrameScala")

    // 创建SparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 导入隐式转换包
    import spark.implicits._

    val deptLine = spark.sparkContext.textFile("spark-learning/data/dept.txt")


    val dept = deptLine.map(line => {
      val arr = line.split(",")
      val deptId = arr(0).toInt
      val deptName = arr(1)
      val deptAddr = arr(2)
      Dept(deptId, deptName, deptAddr)
    })

    val df = dept.toDF()
    df.show()
    df.printSchema()


  }

}
