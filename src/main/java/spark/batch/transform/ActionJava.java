package spark.batch.transform;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * create by liuzhiwei on 2020/4/27
 */
public class ActionJava {
    public static void main(String[] args) {
        //reduce();
        //collect();
        //count();
        //take();
        saveAsTextFile();
    }

    // reduce算子
    public static void reduce() {
        // 创建SparkConf和SparkContext
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("reduce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 构建集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 并行化集合创建初始RDD
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);

        // reduce操作原理，首先将RDD中的第一个和第二个元素传入方法，得到结果，接着将该结果与下一个元素
        // 传入方法，以此类推，reduce的本质就是聚合，将多个元素聚合成一个
        Integer sum = numbersRDD.reduce((x, y) -> x + y);

        System.out.println(sum);

        // 关闭JavaSparkContext
        sc.close();
    }


    /**
     * collect是将远程部署在Spark集群上的RDD上的元素拉到Driver端
     */
    public static void collect() {
        // 创建SparkConf和SparkContext
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("reduce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 构建集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 并行化集合创建初始RDD
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);

        // 使用map将集合元素乘以2
        JavaRDD<Integer> rdd = numbersRDD.map(x -> x * 2);

        // 不用使用foreach action操作，在远程集群上遍历rdd中的元素
        // 使用collect操作，将分布在远程集群上的rdd2的数据拉取到本地
        // 这种方式，一般不建议使用，因为如果rdd中的数据量比较大的话，性能会比较差
        // 因为要从远程走大量的网络传输，将数据获取到本地，还有可能发生oom，内存溢出
        // 因此通常还是建议使用foreach对rdd进行处理
        List<Integer> collect = rdd.collect();
        for (Integer num : collect) {
            System.out.println(num);
        }

        sc.close();
    }

    /**
     * count 统计RDD中元素的个数
     */
    public static void count() {
        // 创建SparkConf和SparkContext
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("reduce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 构建集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 并行化集合创建初始RDD
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);

        long count = numbersRDD.count();
        System.out.println(count);
        // 关闭JavaSparkContext
        sc.close();
    }

    /**
     * take取多少个元素
     */
    public static void take() {
        // 创建SparkConf和SparkContext
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("reduce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 构建集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 并行化集合创建初始RDD
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);

        List<Integer> take = numbersRDD.take(3);
        for (Integer num : take) {
            System.out.println(num);
        }

        // 关闭JavaSparkContext
        sc.close();
    }

    /**
     * saveAsTextFile 可以将rdd的数据保存到hdfs或者本地文件中
     */
    public static void saveAsTextFile() {
        // 创建SparkConf和SparkContext
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("reduce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 构建集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 并行化集合创建初始RDD
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        numbersRDD.saveAsTextFile("spark-learning/data/wc.txt4");

        // 关闭JavaSparkContext
        sc.close();
    }

    /**
     * countByKey对每个key对应的value进行统计
     */
    public static void countByKey() {
        // 创建SparkConf和SparkContext
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("reduce");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 构建集合
        List<Tuple2<String, String>> employees = Arrays.asList(
                new Tuple2<>("dept1", "Allen"),
                new Tuple2<>("dept2", "Bob"),
                new Tuple2<>("dept1", "Carl"),
                new Tuple2<>("dept2", "David")
        );
        JavaPairRDD<String, String> employeeRDD = sc.parallelizePairs(employees);
        // 对rdd应用countByKey操作，统计每个部门的员工人数，也就是统计每个key对应的元素个数
        // 这就是cuntByKey的作用,返回的类型是Map类型
        Map<String, Long> employeess = employeeRDD.countByKey();
        for (Map.Entry<String, Long> e : employeess.entrySet()
        ) {
            System.out.println(e.getKey() + "人数" + e.getValue());
        }
        // 关闭JavaSparkContext
        sc.close();
    }
}

