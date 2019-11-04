import org.apache.spark.{SparkConf, SparkContext}

object CountOfOD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CountOfOD")
    val sc = new SparkContext(conf)

    val ODFile = sc.textFile(args(0)).map(line => {
      val id = line.split(',')(0).drop(1)
      (id, 1)
    })

    val CountOD = ODFile.reduceByKey(_+_).sortBy(_._2, ascending = false)

    CountOD.saveAsTextFile(args(1))
    sc.stop()
  }
}
