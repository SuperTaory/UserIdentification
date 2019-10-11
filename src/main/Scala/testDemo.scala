import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object testDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test-Demo")
    val sc = new SparkContext(conf)
    // val sq = SparkSession.builder().config(conf).getOrCreate()
    // val sq = new SQLContext(sc)

    /*-----------------------------------------------------------------------------------------*/

    // 开始处理OD数据集,包括统计每个站点的客流量和提取某个乘客的OD信息
    val ODFile = sc.textFile(args(0))

    val ODFileRDD = ODFile.filter(_.split(',').length >= 7).map(line => {
      val fields = line.split(',')
      val pid = fields(1)
      val time = fields(4).replace('T', ' ').dropRight(5)
      val station = fields(6)
      (pid, time, station)
    }).cache()

    // 统计每个站点的客流量,按照客流量升序排序并保存
    val PassengerFlowOfStation = ODFileRDD.map(x => (x._3, 1)).reduceByKey(_+_).sortBy(_._2, ascending = true)
    //PassengerFlowOfStation.repartition(1).saveAsTextFile(args(2))

    val minFlow = PassengerFlowOfStation.first()._2.toFloat
    //val maxFlow = PassengerFlowOfStation.take(PassengerFlowOfStation.count().toInt).last._2

    // 对各个站点根据其客流量设置打分参数:rsz
    val RankScoreOfStation = PassengerFlowOfStation.map(line => {
      val score = minFlow / line._2
      (line._1, score)
    })

    // 声明为广播变量用于查询
    val scoreInfo = sc.broadcast(RankScoreOfStation.collect())

    // 提取某个乘客的OD记录
    val personalOD = ODFileRDD.filter(_._1 == args(1))
    personalOD.repartition(1).saveAsTextFile(args(2))

    // 获取该乘客出现过的站点
    val stationAppear = personalOD.map(_._3).distinct().collect()

    //按照站点和用户ID分组，统计一个站点内该ID出现的次数
    val OD_cntRDD = personalOD.groupBy(line => {(line._3, line._1)}).mapValues(v => v.toList.length).map(line=>{
      (line._1._1, (line._1._2, line._2))
    })

    OD_cntRDD.repartition(1).saveAsTextFile(args(3))
    sc.stop()
  }
}
