package spark.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * create by liuzhiwei on 2020/4/22
 * <p>
 * Spark案例：通过并行化集合创建初始RDD
 * <p>
 * 读取数据可以从HDFS、本地文件或者并行化程序集合来创建，本案例是通过并行化集合来创建初始RDD。
 */
public class ParallelizeCollection {
    public static void main(String[] args) {
        //创建sparkConf
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("spark.batch.ParallelizeCollection");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 要通过并行化集合的方式创建RDD，那么就调用SparkContext以及其子类的parallelize()方法
        JavaRDD<Integer> nums = sc.parallelize(numbers);

        // 执行reduce算子操作
        // 相当于，先进行1 + 2 = 3；然后再用3 + 3 = 6；然后再用6 + 4 = 10。。。以此类推
        Integer sum = nums.reduce((x, y) -> x + y);

        System.out.println(sum);

        // 关闭JavaSparkContext
        sc.close();
    }
}
