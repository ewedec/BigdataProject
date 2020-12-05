import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //local模式
    //创建sparkConf对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    conf.set("spark.testing.memory", "2147480000")
    //创建spark上下文
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("hdfs:///spark")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordToOne: RDD[(String, Int)] = words.map((_, 1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    val result: Array[(String, Int)] = wordToSum.collect()
    result.foreach(println)
  }
}