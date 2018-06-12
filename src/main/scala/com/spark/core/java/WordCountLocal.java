package com.spark.core.java;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author zhangjin
 * @create 2018-06-12 18:37
 * 本地测试的wordCount 程序
 */
public class WordCountLocal {

    public static void main(String[] args) {
        //编写spark应用程序
        //第一步 ： 创建SparkConf对象 设置spark应用的配置信息
        //master 使用 setmaster 可以设置 相当于Spark应用程序要连接的Spark集群的Master节点的URL
        //但是如果设置为local 则代表 在本地运行 ok
        SparkConf conf = new SparkConf()
                .setAppName(WordCountLocal.class.getSimpleName())
                .setMaster("local[1]");

        //第二步 创建 JavaSparkContext对象
        //在spark中 SparkContext是spark所有功能的入口 你无论是使用java scala 还是python编写
        //都必须要有一个SparkContext 它的主要作用 就是初始化Spark应用程序所需的一些核心组件，包括
        //调度器 DAGSchedule TaskScheduler 还回去到Spark master节点进行注册 等待
        //SparkContext是Spark应用中 最最重要的一个对象
        //但是 在Spark中 编写不同类型的Spark应用程序 所使用的SparkContext是不同的 如果使用的是Scala
        // 使用的就是原生的SparkContext对象 但是如果使用Java 那么就是Java的JavaSparkContext对象
        //如果使用的Spark SQL程序 那么就是 SQLContext 或者 HiveContext
        //如果开发的是Spark Streaming 那木就是它独有的SparkContext 依次类推
        JavaSparkContext sc = new JavaSparkContext(conf);

        //第三部 要针对输入源 hdfs 本地文件 等等  创建一个初始的RDD RDD就是分散在Spark各个节点上的数据集
        //输入源的数据会打散 分配到RDd的每个Patition中 从而形成一个初始的分布式的数据集
        // SparkContext 用于根据文件类型的输入源 创建RDD的方法 叫做TextFile（）方法

        //在这里呢 RDD中 有元素这种概念 如果是hdfs 或者 本地文件呢 创建的RDD 每一个元素就相当于是文件里的一行
        JavaRDD<String> lines = sc.textFile("/Users/zhangjin/myCode/learn/spark-study/input/wc.txt");

        // 第四步 对初始RDD进行transformation操作 也就是一些计算操作

        //通常操作 会通过创建function 并配合RDD的map ，flatmap等算子来执行
        //function 通过 如果比较简单
        // 先将每一行 拆分成 单个的单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });



        // 接着，需要将每一个单词，映射为(单词, 1)的这种格式
        // 因为只有这样，后面才能根据单词作为key，来进行每个单词的出现次数的累加
        // mapToPair，其实就是将每个元素，映射为一个(v1,v2)这样的Tuple2类型的元素
        // 如果大家还记得scala里面讲的tuple，那么没错，这里的tuple2就是scala类型，包含了两个值
        // mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型
        // 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型
        // JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
        JavaPairRDD<String, Integer> pairs = words.mapToPair(

                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        return new Tuple2<String, Integer>(word, 1);
                    }

                });

        // 接着，需要以单词作为key，统计每个单词出现的次数
        // 这里要使用reduceByKey这个算子，对每个key对应的value，都进行reduce操作
        // 比如JavaPairRDD中有几个元素，分别为(hello, 1) (hello, 1) (hello, 1) (world, 1)
        // reduce操作，相当于是把第一个值和第二个值进行计算，然后再将结果与第三个值进行计算
        // 比如这里的hello，那么就相当于是，首先是1 + 1 = 2，然后再将2 + 1 = 3
        // 最后返回的JavaPairRDD中的元素，也是tuple，但是第一个值就是每个key，第二个值就是key的value
        // reduce之后的结果，相当于就是每个单词出现的次数
        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }

                });

        // 到这里为止，我们通过几个Spark算子操作，已经统计出了单词的次数
        // 但是，之前我们使用的flatMap、mapToPair、reduceByKey这种操作，都叫做transformation操作
        // 一个Spark应用中，光是有transformation操作，是不行的，是不会执行的，必须要有一种叫做action
        // 接着，最后，可以使用一种叫做action操作的，比如说，foreach，来触发程序的执行
        wordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {

            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared " + wordCount._2 + " times.");
            }

        });

        sc.close();

    }


}
