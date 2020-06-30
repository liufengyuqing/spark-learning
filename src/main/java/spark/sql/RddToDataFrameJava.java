package spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * create by liuzhiwei on 2020/5/12
 */
public class RddToDataFrameJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("RddToDataFrameJava");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .getOrCreate();

        JavaRDD<String> dept = spark.sparkContext().textFile("spark-learning/data/dept.txt", 1).toJavaRDD();

        JavaRDD<DeptBean> deptBeanJavaRDD = dept.map(line -> {
            String[] arr = line.split("\t");
            DeptBean deptBean = new DeptBean();
            int deptid;
            if (!arr[0].isEmpty()) {
                deptid = Integer.valueOf(arr[0]);
            } else {
                deptid = 0;
            }
            deptBean.setDeptId(deptid);
            deptBean.setDeptName(arr[1]);
            deptBean.setDeptAddr(arr[2]);
            return deptBean;
        });


        Dataset<Row> deptDF = spark.createDataFrame(deptBeanJavaRDD, DeptBean.class);

        deptDF.printSchema();
        deptDF.show();

        RDD<Row> rdd = deptDF.rdd();

    }
}
