import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.math.abs

object DivisionAndCompletion {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DivisionAndCompletion")
    val sc = new SparkContext(conf)

    val readMacFile = sc.textFile(args(0)).map(line => {
      val fields = line.split(',')
      val macId = fields(0).drop(1)
      val time = fields(1).toLong
      val station = fields(2).dropRight(1)
      (macId, (time, station))
    }).groupByKey().mapValues(_.toList.sortBy(_._1))

    val readODTimeInterval = sc.textFile(args(1)).map(line => {
      val p = line.split(',')
      val sou = p(0).drop(1)
      val des = p(1)
      val interval = p(2).dropRight(1).toLong
      ((sou, des), interval)
    })
    val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)


    val divisionRDD = readMacFile.flatMap(line => {
      val MacId = line._1
      val data = line._2
      val segement = new ListBuffer[(Long, String)]
      val segements = new ListBuffer[List[(Long, String)]]
      for (s <- data) {
        if (segement.isEmpty){
          segement.append(s)
        }
        else {
          if (s._2 == segement.last._2){
            segements.append(segement.toList)
            segement.clear()
          }
          else if (abs(s._1 - segement.last._1) > ODIntervalMap.value((segement.last._2, s._2)) + 600) {
            segements.append(segement.toList)
            segement.clear()
          }
          segement.append(s)
        }
      }
      segements.append(segement.toList)
      for (seg <- segements) yield {
        (MacId, seg)
      }
    })

    divisionRDD.repartition(1).sortBy(x => (x._1, x._2.head._1)).saveAsTextFile(args(2))

    sc.stop()

  }
}
