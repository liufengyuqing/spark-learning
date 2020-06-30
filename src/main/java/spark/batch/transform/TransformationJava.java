package spark.batch.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * create by liuzhiwei on 2020/4/27
 */
public class TransformationJava {
    public static void main(String[] args) {
        //groupByKey();
        //reduceByKey();
        sortByKey();
    }

    /**
     * groupByKey案例，根据不同部门的工资进行分组
     */
    public static void groupByKey() {
        // 创建SparkConf对象
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("groupByKey");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建集合
        List<Tuple2<String, Integer>> salary = Arrays.asList(
                new Tuple2<>("dept1", 1000),
                new Tuple2<>("dept2", 11000),
                new Tuple2<>("dept1", 2000),
                new Tuple2<>("dept2", 21000),
                new Tuple2<>("dept1", 11000));

        /**
         * 并行化集合创建RDD
         * 在java中并行化集合有两种方法,
         * 一种parallelize()方法，创建普通JavaRDD，
         * 一种是parallelizePairs()方法，创建JavaPairRDD,也就是<k,v>类型的RDD
         * 在Scala中都是parallelize()方法创建
         */
        JavaPairRDD<String, Integer> salartRDD = sc.parallelizePairs(salary);

        JavaPairRDD<String, Iterable<Integer>> rdd2 = salartRDD.groupByKey();

        rdd2.foreach(x -> {
            System.out.println(x);
            Iterator<Integer> iterator = x._2.iterator();
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        });
        // 关闭JavaSparkContext
        sc.close();
    }

    /**
     * reduceByKey案例，计算不同部门的员工工资和
     */
    public static void reduceByKey() {
        // 创建SparkConf对象
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("groupByKey");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建集合
        List<Tuple2<String, Integer>> salary = Arrays.asList(
                new Tuple2<>("dept1", 1000),
                new Tuple2<>("dept2", 11000),
                new Tuple2<>("dept1", 2000),
                new Tuple2<>("dept2", 21000),
                new Tuple2<>("dept1", 11000));

        /**
         * 并行化集合创建RDD
         * 在java中并行化集合有两种方法,
         * 一种parallelize()方法，创建普通JavaRDD，
         * 一种是parallelizePairs()方法，创建JavaPairRDD,也就是<k,v>类型的RDD
         * 在Scala中都是parallelize()方法创建
         */
        JavaPairRDD<String, Integer> salartRDD = sc.parallelizePairs(salary);

        // 执行reduceByKey算子，根据部门计算工资和
        JavaPairRDD<String, Integer> rdd2 = salartRDD.reduceByKey((x, y) -> (x + y));

        rdd2.foreach(x -> System.out.println("部门： " + x._1 + "的工资和：" + x._2));
        // 关闭JavaSparkContext
        sc.close();
    }


    /**
     * sortByKey案例，按照工资进行排序
     */
    public static void sortByKey() {
        // 创建SparkConf对象
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("groupByKey");

        // 创建JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 创建集合
        List<Tuple2<Integer, String>> salary = Arrays.asList(
                new Tuple2<>(10000, "dept1"),
                new Tuple2<>(11000, "dept2"),
                new Tuple2<>(13000, "dept1"),
                new Tuple2<>(12000, "dept2"));

        /**
         * 并行化集合创建RDD
         * 在java中并行化集合有两种方法,
         * 一种parallelize()方法，创建普通JavaRDD，
         * 一种是parallelizePairs()方法，创建JavaPairRDD,也就是<k,v>类型的RDD
         * 在Scala中都是parallelize()方法创建
         */
        JavaPairRDD<Integer, String> salartRDD = sc.parallelizePairs(salary);


        // 执行sortByKey算子,默认是按照key升序排列，可以在sortByKey加上false参数变成降序
        JavaPairRDD<Integer, String> rdd2 = salartRDD.sortByKey(false);

        rdd2.foreach(x -> System.out.println(x));
        // 关闭JavaSparkContext
        sc.close();

    }

}
