package spark.streaming

import java.util.Random

/**
 * create by liuzhiwei on 2020/4/25
 * 自定义Socket数据源
 *
 */
object DataSourceSocket {
  def index(length: Int): Unit = {
    val rdm = new Random()
    rdm.nextInt(length)
  }

  def main(args: Array[String]): Unit = {

  }

}
