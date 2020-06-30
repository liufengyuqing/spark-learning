package spark.batch.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * create by liuzhiwei on 2020/4/27
 */
public class TransformationJoinJava {
    public static void main(String[] args) {
       // join();
        cogroup();
    }

    public static void join() {
        // 创建SparkConf对象
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("groupByKey");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建员工集合
        List<Tuple2<Integer, String>> employeeList = Arrays.asList(
                new Tuple2<>(1, "Allen"),
                new Tuple2<>(2, "Bob"),
                new Tuple2<>(3, "Carl"));

        //创建工资集合 这里为了说明join和cogroup的区别，假设每个员工有2份收入，一份工资，一份奖金
        List<Tuple2<Integer, Integer>> salaryList = Arrays.asList(
                new Tuple2<>(1, 10000),
                new Tuple2<>(1, 5000),
                new Tuple2<>(2, 11000),
                new Tuple2<>(2, 6000),
                new Tuple2<>(3, 12000),
                new Tuple2<>(3, 6000));

        // 两个集合并行化生成两个RDD
        JavaPairRDD<Integer, String> employeeRDD = sc.parallelizePairs(employeeList);
        JavaPairRDD<Integer, Integer> salaryRDD = sc.parallelizePairs(salaryList);

        // 使用join算子将两个RDD关联起来，打印每一个员工的id，工资，类似于关系型数据库的join
        JavaPairRDD<Integer, Tuple2<String, Integer>> join = employeeRDD.join(salaryRDD);

        // 打印
        join.foreach(x -> System.out.println(x));
        // 关闭JavaSparkContext
        sc.close();
    }


    public static void cogroup() {
        // 创建SparkConf对象
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("groupByKey");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        //创建员工集合
        List<Tuple2<Integer, String>> employeeList = Arrays.asList(
                new Tuple2<>(1, "Allen"),
                new Tuple2<>(2, "Bob"),
                new Tuple2<>(3, "Carl"));

        //创建工资集合 这里为了说明join和cogroup的区别，假设每个员工有2份收入，一份工资，一份奖金
        List<Tuple2<Integer, Integer>> salaryList = Arrays.asList(
                new Tuple2<>(1, 10000),
                new Tuple2<>(1, 5000),
                new Tuple2<>(2, 11000),
                new Tuple2<>(2, 6000),
                new Tuple2<>(3, 12000),
                new Tuple2<>(3, 6000));

        // 两个集合并行化生成两个RDD
        JavaPairRDD<Integer, String> employeeRDD = sc.parallelizePairs(employeeList);
        JavaPairRDD<Integer, Integer> salaryRDD = sc.parallelizePairs(salaryList);

        // 使用cogroup算子将两个RDD关联起来，打印每一个员工的id，工资
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = employeeRDD.cogroup(salaryRDD);

        //打印
        cogroup.foreach(x -> System.out.println(x));

        // 关闭JavaSparkContext
        sc.close();


    }


}
