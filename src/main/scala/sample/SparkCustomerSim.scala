package sample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

case class CustomerSimStart(customerId:Int, simId:String, startDate:String)
case class CustomerSimInterval(customerId:Int, simId:String, startDate:String, endDate:Option[String])

object SparkCustomerSim {

  val localPath = """C:\workspace\starter-spark-scala-maven\src\main\resources\"""

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName("SparkOrderAggregator")
      .master("local")
      .getOrCreate()

    val lines = spark.read.textFile(localPath + """customer_sim_input.txt""").rdd

    val customers: RDD[(String, Iterable[CustomerSimStart])] = lines.map{ s =>
      val parts = s.split("\\s+")
      CustomerSimStart(parts(0).toInt, parts(1), parts(2))
    }.distinct().groupBy(_.simId).cache()



    def f(iterable: Iterable[CustomerSimStart]) = {
      val sorted = iterable.toArray.sortBy(_.startDate)
      val lastOwner = sorted.last
      sorted.sliding(2).map{
        case Array(a,b) => CustomerSimInterval(a.customerId, a.simId, a.startDate, Some(b.startDate))
      }.toArray ++ Array(CustomerSimInterval(lastOwner.customerId, lastOwner.simId, lastOwner.startDate, None))
    }

    val y: RDD[String] = customers.mapValues(f).flatMap(entry => entry._2).map(c => s"""${c.customerId} ${c.simId} ${c.startDate} ${c.endDate}""")

    y.saveAsTextFile(localPath+"""customer_output""")


    val callRdd = spark.read.textFile(localPath + "call_data.txt").rdd
    val bySim = callRdd.map(CellCallData(_)).groupBy(_.simId).cache()

    bySim.mapValues(simData => simData.groupBy(_.phoneNumber).mapValues())

//    Thread.sleep(1000*1000)
    spark.stop()
  }
}

case class CellCallData(cellId: String, simId:String, phoneNumber:String, seconds:Int)
object CellCallData {
  private val LinePattern = """(\w+)\s(\w+)\s(\w+)\s(\d+)""".r
  def apply(csvLine: String):CellCallData = {
      val LinePattern(cell, sim, number, seconds) = csvLine
      CellCallData(cell,sim,number,seconds.toInt)
  }
}

//object CellCallDataPattern {
//  def unapply(arg: CellCallDataPattern): Option[] = ???
//}