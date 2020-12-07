import org.apache.spark.{SparkConf, SparkContext}

object TransformationOp {
  val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transformation_op");
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 2)
      .mapPartitions(iter => {
        iter.map(item => item * 10)
        iter
      }).collect()
      .foreach(item => println(item))
    mapPartitionsWithIndex()
  }

  def mapPartitionsWithIndex(): Unit = {
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), 2)
      .mapPartitionsWithIndex((index, iter)=>{
      println("index" + index)
        iter.foreach(item=>println(item))
        iter
        })
      .collect()
  }

}
