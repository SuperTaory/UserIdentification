import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object JunctionPair {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JunctionPair")
    val sc = new SparkContext(conf)

    val junctionSet = Set("宝安中心", "前海湾", "世界之窗", "车公庙", "购物公园", "会展中心", "大剧院", "老街", "后海", "安托山", "景田", "福田", "市民中心",
      "华强北", "黄贝岭", "石厦", "少年宫", "华新", "红岭", "田贝", "布吉", "深圳北站", "上梅林", "福民", "西丽", "太安", "红岭北", "红树湾南", "机场东", "罗湖",
      "赤湾", "新秀", "双龙", "益田", "清湖", "福田口岸", "西丽湖", "文锦", "碧头")


    val stationInfo = sc.textFile(args(0))
    val nameAsKey = stationInfo.map(line => {
      val fields = line.split(',')
      val Name = fields(1)
      val StationNo = fields(0)
      val LineNo = fields(5) match {
        case "268" => "1"
        case "260" => "2"
        case "261" => "3"
        case "262" => "4"
        case "263" => "5"
        case "265" => "7"
        case "267" => "9"
        case "241" => "11"
      }
      (Name, (StationNo, LineNo))
    })
    val stationMap1 = sc.broadcast(nameAsKey.collect().toMap)

    val stationNoAsKey = stationInfo.map(line => {
      val fields = line.split(',')
      val Name = fields(1)
      val StationNo = fields(0)
      val LineNo = fields(5)
      (StationNo, (Name, LineNo))
    })
    val stationMap2 = sc.broadcast(stationNoAsKey.collect().toMap)

    val transferStation = sc.textFile(args(1)).map(line => {
      val fields = line.split(',')
      val name = fields(0)
      val belongLine = fields.drop(1).toSet
      (name, belongLine)
    })
    val transferStationMap = sc.broadcast(transferStation.collect().toMap)


    val validPathsFile = sc.textFile(args(2)).map(line => {
      val fields = line.split(' ')
      val path = fields.dropRight(5)
      val sou = stationMap2.value(path.head)._1
      val des = stationMap2.value(path.last)._1
      val pathWithName = new ListBuffer[String]
      path.foreach(x => pathWithName.append(stationMap2.value(x)._1))
      ((sou, des), pathWithName.toList)
    })

    val filterJunctionPair = validPathsFile.filter(line => {
      var flag1 = false
      var flag2 = true
      if (junctionSet.contains(line._1._1) && junctionSet.contains(line._1._2))
        flag1 = true
      if (line._2.length > 2) {
        val midPart = line._2.drop(1).dropRight(1)
        for (s <- midPart) {
          if (junctionSet.contains(s))
            flag2 = false
        }
      }
      flag1 && flag2
    }).filter(x => x._1._1 != x._1._2)

    val result = filterJunctionPair.map(line => {
      var lineNo = "-1"
      if (line._2.length > 2) {
        lineNo = stationMap1.value(line._2(1))._2
      }
      else {
        if (!transferStationMap.value.keySet.contains(line._1._1)){
          lineNo = stationMap1.value(line._1._1)._2
        }
        else if (!transferStationMap.value.keySet.contains(line._1._2)){
          lineNo = stationMap1.value(line._1._2)._2
        }
        else {
          val commonLine = transferStationMap.value(line._1._1).intersect(transferStationMap.value(line._1._2))
          if (commonLine.size == 1)
            lineNo = commonLine.head
          else
            lineNo = "11"  //针对深圳市地铁线路中车公庙和红树湾南的特殊情况，两站为相邻站时为11号线
        }
      }
      var path = ""
      line._2.foreach(x => path += x + ",")
      (line._1._1, line._1._2, lineNo, path.dropRight(1))
    }).filter(_._3 != "-1").repartition(1).sortBy(x => (x._1, x._2))

    result.saveAsTextFile(args(3))

//    filterJunctionPair.repartition(1).sortBy(x => (x._1)).saveAsTextFile(args(3))
    sc.stop()

  }
}
