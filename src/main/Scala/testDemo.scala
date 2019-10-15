import java.text.SimpleDateFormat
import scala.math.max

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map, Set}

object testDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test-Demo")
    val sc = new SparkContext(conf)
    // val sq = SparkSession.builder().config(conf).getOrCreate()
    // val sq = new SQLContext(sc)

    /*-----------------------------------------------------------------------------------------*/
    // 读取地铁站点名和编号映射关系
    val stationFile = sc.textFile(args(0))
    val stationNoToName = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo, stationName)
    }).collect().toMap

    val stationNameToNo = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationName, stationNo)
    }).collect().toMap

    // 读取所有有效路径的数据
    val validPathFile = sc.textFile(args(1)).map(line => {
      // 仅保留站点编号信息
      val fields = line.split(' ').dropRight(5)
      val sou = fields(0)
      val des = fields(fields.length-1)
      ((sou, des), fields)
    })

    // 键-OD站点编号，值-OD之间的所有有效路径，由编号组成
    val validPathSet = validPathFile.groupByKey().mapValues(_.toList).collect().toMap

    // 读取乘客的OD记录
    val personalOD = sc.textFile(args(2)).map(line => {
      val fields = line.split(',')
      val pid = fields(0)
      val time = transTimeFormat(fields(1))
      val station = fields(2)
      val tag = fields(3)
      (pid, time, station, tag)
    }).sortBy(_._2, ascending = true).collect()

    /*-----------------------------------------------------------------------------------------*/

    // 统计OD包含的有效路径经过的站点集合（去重）
    val ODStationSet : mutable.Set[(String, String)] = mutable.Set()
    var index = 0
    while (index + 1 < personalOD.length)
    {
      if (personalOD(index)._4 == "21" && personalOD(index+1)._4 == "22")
      {
        // 把站点名转换为编号
        val so = personalOD(index)._3
        val sd = personalOD(index+1)._3
        ODStationSet.add((stationNameToNo(so), stationNameToNo(sd)))
        index += 1
      }
      index += 1
    }

    val validPathStationNoSet : mutable.Set[String] = mutable.Set()
    ODStationSet.foreach(x => {
      val vp = validPathSet(x)
      for (v <- vp.indices)
      {
        validPathStationNoSet.++=(vp(v).toSet)
      }
    })

    val validPathStationNameSet : mutable.Set[String] = mutable.Set()
    validPathStationNoSet.foreach(x =>{
      validPathStationNameSet.add(stationNoToName(x))
    })
//    print(validPathStationNameSet)

    /*-----------------------------------------------------------------------------------------*/

    // 统计每个MacID对应的站点集合
    val macFile = sc.textFile(args(3))

    val macRDD = macFile.map(line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = fields(1).toLong
      val station = fields(2).dropRight(1)
      (macId, (time, station))
    }).cache()

    val macStation_Only = macRDD.map(x=>(x._1, x._2._2)).groupByKey().mapValues(_.toSet)

    /*-----------------------------------------------------------------------------------------*/

    // 过滤出候选集合
    // 15308762 -> 2572154
    // 15308762 -> 29493 (添加设置size比较时)
    val candidateMacSet = macStation_Only.filter(x => validPathStationNameSet.union(x._2).equals(validPathStationNameSet) &&
    x._2.size >= validPathStationNameSet.size / 5)
    //println("candidates:" + candidateMacSet.count())
    //candidateMacSet.saveAsTextFile(args(4))

    // 处理候选集mac对应的信息备用
    val candidateMacIds = candidateMacSet.map(x => x._1).collect().toSet
    val candidateMacInfo = macRDD.filter(x => candidateMacIds.contains(x._1))
    val formatMacInfo = candidateMacInfo.groupByKey().mapValues(_.toList.sortBy(_._1))

    // 处理OD数据备用,主要是把有效路径的站点编号转换为名称
    var perODMap : mutable.Map[(String, String), List[List[String]]] = mutable.Map()
    ODStationSet.foreach(x => {
      val od = (stationNoToName(x._1), stationNoToName(x._2))
      val vp = validPathSet(x)
      val paths = new ListBuffer[List[String]]
      for (v <- vp){
        val path = new ListBuffer[String]
        for (p <- v){
          path.append(stationNoToName(p))
        }
        paths.append(path.toList)
      }
      perODMap += (od -> paths.toList)
    })

    /*-----------------------------------------------------------------------------------------*/
    // 比较每一个macInfo和当前乘客的OD记录计算相似度
    val rankRDD = formatMacInfo.map(line => {
      val macID = line._1
      val macInfo = line._2
      var score = 0
      var index = 0
      while (index + 1 < personalOD.length)
      {
        if (personalOD(index)._4 == "21" && personalOD(index+1)._4 == "22" && personalOD(index+1)._2 - personalOD(index)._2 < 10800)
        {
          val so = personalOD(index)._3
          val sd = personalOD(index+1)._3
          val to = personalOD(index)._2
          val td = personalOD(index+1)._2
          val paths = perODMap((so, sd))
          val start = macInfo.indexWhere(_._1 > to - 300)
          val end = macInfo.lastIndexWhere(_._1 < td + 300)
          if (end >= start && start >= 0){
            var temp_score = 0
            var index_mac = start
            for (path <- paths){
              var path_score = 0
              for (station <- path if index_mac <= end){
                if (macInfo(index_mac)._2.equals(station)) {
                  index_mac += 1
                  path_score += 1
                }
              }
              if (index_mac == end + 1){
                temp_score = max(temp_score, path_score)
              }
            }
            score += temp_score
          }
          index += 1
        }
        index += 1
      }
      (macID, score)
    }).sortBy(_._2, ascending = false)

    rankRDD.saveAsTextFile(args(4))

    sc.stop()
  }

  def transTimeFormat(timeString : String) : Long = {
    val pattern = "yyyy-MM-dd HH:mm:ss"
    val dateFormat = new SimpleDateFormat(pattern)
    val time = dateFormat.parse(timeString).getTime / 1000
    time
  }
}
