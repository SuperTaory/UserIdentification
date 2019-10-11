// import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.SQLContext

import scala.collection.mutable.ListBuffer
import scala.math.{max, min, pow}

object PreProcessing {
  def main(args: Array[String]): Unit = {
    //Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("Pre-Processing")
    val sc = new SparkContext(conf)
    //val sq = SparkSession.builder().config(conf).getOrCreate()
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

    // 获取该乘客出现过的站点
    val stationAppear = personalOD.map(_._3).distinct().collect()

    //按照站点和用户ID分组，统计一个站点内该ID出现的次数
    val OD_cntRDD = personalOD.groupBy(line => {(line._3, line._1)}).mapValues(v => v.toList.length).map(line=>{
      (line._1._1, (line._1._2, line._2))
    })
    OD_cntRDD.repartition(1).saveAsTextFile(args(2))


    /*-----------------------------------------------------------------------------------------*/

    // 将mac数据文件格式由parquet转换为普通格式，并对mac数据进行处理压缩；而OD数据不需要进行压缩处理
    // 原Mac数据格式：[000000000140,1559177788,八卦岭,114.09508148721412,22.56193897883047,9]
    // 转换后Mac格式：000000000140,1559177788,八卦岭

    /*-----------------------------------------------------------------------------------------*/
//
//    // 开始处理Mac数据集
//    // 读取mac数据并转换为数据库Table格式
//    val macFile = sq.read.parquet(args(0))
//    macFile.printSchema()
//    val macSchema = macFile.schema
////    macFile.createOrReplaceTempView("MacTable")
//    macFile.registerTempTable("MacTable")
//    // 1559318400是指2019/06/01 00:00:00对应的unix时间戳
//    val macRDD = sq.sql("select * from MacTable where " + macSchema.fieldNames(1) + " > 1559318400").rdd
//
//    val transformedMacRDD = macRDD.map(line => {
//      val fields = line.toString().split(',')
//      val macId = fields(0).drop(1)
//      val time = fields(1)
//      val station = fields(2)
//      (macId, time, station)
//    }).filter(x =>{stationAppear.contains(x._3)}).sortBy(line => (line._1, line._2), ascending = true)
//    //println("before compression:" + transformedMacRDD.count())
//    //transformedMacRDD.saveAsTextFile(args(1))
//
//
//    // 过滤掉同一站点相邻时刻重复检测到的mac信息
//    var baseTime = "0"
//    var baseStation = "null"
//    val limitTime = 600 // 10min = 600sec
//    val compressionRDD = transformedMacRDD.filter(line =>{
//      if (line._3 == baseStation && line._2.toInt < baseTime.toInt + limitTime)
//        false
//      else{
//        baseStation = line._3
//        baseTime = line._2
//        true
//      }
//    })
//    //println("after compression:" + compressionRDD.count())
//    //compressionRDD.saveAsTextFile(args(2))

    // 读取所有去重后的mac信息
    val macFile = sc.textFile(args(3))
    val compressionRDD = macFile.map(line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = fields(1)
      val station = fields(2).dropRight(1)
      (macId, time, station)
    }).filter(x =>{stationAppear.contains(x._3)})

    // 按照站点和macID分组，统计一个站点内该macID出现的次数
    val macID_cntRDD = compressionRDD.groupBy(line => {(line._3, line._1)}).mapValues(v => v.toList.length).map(line=>{
      (line._1._1, (line._1._2, line._2))
    })
    //macID_cntRDD.saveAsTextFile(args(3))

    /*-----------------------------------------------------------------------------------------*/

    // 生成(TA.id, (TB.id, station, cnt))
    val mergedStaCnt = OD_cntRDD.leftOuterJoin(macID_cntRDD).map(line => {
      (line._2._1._1, (line._2._2.get._1, line._1, min(line._2._1._2, line._2._2.get._2)))
    })

    //mergedStaCnt.saveAsTextFile(args(3))

    /*-----------------------------------------------------------------------------------------*/

    // (景田,612565)
    // (深大,610612)
    // (福永,526212)
    // (塘朗,521382)
    // Increase the ranking score of TB.id by (rsz * o)
    val CandidateSet = mergedStaCnt.map(line => {
      val station = line._2._2
      val rsz =  scoreInfo.value.toMap.get(station).get
      val score = line._2._3 * rsz
      (line._1, line._2._1, line._2._2, line._2._3, score)
    }).cache()

    //val topQ_Candidates = sc.makeRDD(CandidateSet)
    //topQ_Candidates.repartition(1).saveAsTextFile(args(3))
    CandidateSet.saveAsTextFile(args(4))

    /*-----------------------------------------------------------------------------------------*/
    // 读取站点经纬度信息
    val stationFile = sc.textFile(args(5))
    val stationRDD = stationFile.map(line => {
      val fields = line.split(',')
      val stationName = fields(1)
      val lat = fields(2).toDouble
      val lon = fields(3).toDouble
      (stationName, (lon, lat))
    })

    // 声明为广播变量，用于根据站点名查询经纬度
    val stationInfo = sc.broadcast(stationRDD.collect())

    // 转换为(TA.id, TB.id, list(TA.id, TB.id, station, cnt, score)),并过滤出参与计算的候选集合
    val groupedRDD = CandidateSet.groupBy(x => (x._1, x._2)).mapValues(_.toList).filter(_._2.length >= stationAppear.length-1)

    // 转换为(TA.id, TB.id, SIG)
    val transformedRDD = groupedRDD.map(line => {
      val TAID = line._1._1
      val TBID = line._1._2
      val co_occurrences = line._2
      var ob : Map[String, Double] = Map()
      var st : Map[String, Double] = Map()
      val stationMap = stationInfo.value.toMap
      val K = new ListBuffer[String]
      var md : Map[String, Double] = Map()
      for (x <- co_occurrences.indices){
        ob += (co_occurrences(x)._3 -> sigmoidFunction(co_occurrences(x)._4, 16, 0.2))

        // 初始化第一个站点的st值
        if (x == 0)
          st += (co_occurrences(x)._3 -> ob(co_occurrences(x)._3))
        else{
          if (K.isEmpty)
            st +=(co_occurrences(x)._3 -> max(0.0, ob(co_occurrences(x)._3)))
          else{
            var tempResult = 0.0
            var min_distance = 10000.0
            val gps_x = stationMap.get(co_occurrences(x)._3).get
            for (l <- K.indices){
              val gps_l = stationMap.get(K(l)).get
              val dist_l_x = getDistance(gps_l._1, gps_l._2, gps_x._1, gps_x._2)
              min_distance = min(min_distance, dist_l_x)
              tempResult += (st(K(l)) * pow(0.4, dist_l_x))
            }
            md += (co_occurrences(x)._3 -> min_distance)
            st += (co_occurrences(x)._3 -> max(0.0, ob(co_occurrences(x)._3) - tempResult))
          }
        }
        if (st(co_occurrences(x)._3) > 0)
          K.append(co_occurrences(x)._3)
      }

      var SIG = 0.0
      SIG = st(co_occurrences.head._3)
      if(K.nonEmpty){
        for (x <- K.indices){
          if(K(x) != co_occurrences.head._3)
            SIG += (st(K(x)) * (1 + sigmoidFunction(md(K(x)), 50, 1.0/4000)))
        }
      }
      (TAID, TBID, SIG)
    }).sortBy(_._3, ascending = false)

    transformedRDD.saveAsTextFile(args(6))

    sc.stop()
  }

  def sigmoidFunction(v : Double, para1 : Double, para2 : Double) : Double = {
    val denominator = 1 + math.exp(-v * para2)
    para1 / denominator - para1 / 2
  }

  // 计算GPS距离，单位为km
  def getDistance(lng1: Double, lat1: Double, lng2: Double, lat2: Double): Double = {
    val EARTH_RADIUS = 6378.137
    val radLat1 = lat1 * Math.PI / 180.0
    val radLat2 = lat2 * Math.PI / 180.0
    val a = radLat1 - radLat2
    val b = lng1 * Math.PI / 180.0 - lng2 * Math.PI / 180.0
    var s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)))
    s = s * EARTH_RADIUS
    s
  }

}
