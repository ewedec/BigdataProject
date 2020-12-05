import org.apache.commons.lang.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

object AccessLogAgg {
  def main(args: Array[String]): Unit = {

    //创建SparkContext
    val conf = new SparkConf().setMaster("local[6]").setAppName("ip_agg")
    val sc = new SparkContext(conf)

    //2、读取文件，生成数据集
    val sourceRDD = sc.textFile("hdfs:///spark/access_log_sample.txt")

    //3、取出IP，生产数据集

    val ipRDD = sourceRDD.map(item => (item.split(" ")(0), 1))

    //4、简单清洗
    // 4.1去掉空数据
    // 4.2去掉非法的数据
    // 4.3根据业务再规整一下数据
    val cleanRDD = ipRDD.filter(item => StringUtils.isNotEmpty(item._1))
    //5.根据IP出现次数进行聚合
    val ipAggRDD = cleanRDD.reduceByKey((curr, agg) => curr + agg)
    //6.根据IP出现的次数进行排序
    val sortedRDD = ipAggRDD.sortBy(item => item._2, ascending = false)
    //7.取出结果，打印结果
    sortedRDD.take(num = 10).foreach(item => println(item))
  }

}
