import GeneralFunctionSets.{dayOfMonth_long, transTimeToTimestamp}
import org.apache.spark.sql.SparkSession

object TongJiAFC {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName("SplitAPData")
            .getOrCreate()
        val sc = spark.sparkContext

        //(669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
        val AFCFile = sc.textFile(args(0) + "/Destination/subway-pair/*").map(line => {
            val fields = line.split(',')
            val id = fields(0).drop(1)
            val ot = transTimeToTimestamp(fields(1))
            val os = fields(2)
            val dt = transTimeToTimestamp(fields(4))
            val ds = fields(5)
            val o_day = dayOfMonth_long(ot)
            val d_day = dayOfMonth_long(dt)
            val day = if (o_day == d_day) o_day else 0
            (id, (ot, os, dt, ds, day))
        }).filter(x => x._2._5 > 0 & x._2._5 <= 15)


        val individual = AFCFile.groupByKey().mapValues(line => {
            val v = line.toArray
            val days = v.map(_._5).toSet
            (v.length, days.size)
        }).cache()

        val passengers = individual.count()
        val trips = individual.map(x => (x._2._1, 1)).reduceByKey(_+_).collect()
        val totalTrips = trips.map(x => x._1 * x._2).sum
        val active_days = individual.map(x => (x._2._2, 1)).reduceByKey(_+_).collect()
        val everyDay = AFCFile.map(x => (x._2._5, 1)).reduceByKey(_+_).collect()

        println(passengers)
        println(totalTrips)
        println(trips.sortBy(_._1).mkString("Array(", ", ", ")"))
        println(active_days.sortBy(_._1).mkString("Array(", ", ", ")"))
        println(everyDay.sortBy(_._1).mkString("Array(", ", ", ")"))

        sc.stop()
    }
}
