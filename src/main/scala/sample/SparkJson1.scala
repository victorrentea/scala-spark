//package sample
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//
//import scala.collection.mutable
//
//object SparkJson1 {
//
//  val localPath = """C:\workspace\starter-spark-scala-maven\src\main\resources\"""
//
//  def main(args: Array[String]) {
//
//    val spark = SparkSession
//      .builder
//      .appName("SparkOrderAggregator")
//      .master("local")
//      .getOrCreate()
//
//    val frame: DataFrame = spark.read
//      .option("multiLine", true)
//      .option("mode", "PERMISSIVE")
//      .json(localPath+"""customer_data.json""")
//    val lines: RDD[Row] = frame.rdd
//
////    lines.foreach(row => println(row))
////    lines.foreach(row => println())
////    lines.foreach(row => println())
//    case class CustomerSimStart(customerId:Int, simId:String, startDate:String)
//    case class CustomerSimInterval(customerId:Int, simId:String, startDate:String, endDate:Option[String])
//
//
//    def customerId(row: Row) = row.getAs[Int]("customerId")
//    def sims(row: Row):Array[(String, String)] = {
//      println(row.getAs[GenericRowWithSchema]("sims").schema)
//      throw new IllegalArgumentException
//    }
////    def simsConvert(customerId: Int, sims: Array[(String, String)]): Array[CustomerSimStart] = {
////      def ff =
////
////      sims.map({
////        case (simId, startDate) => CustomerSimStart(customerId, simId, startDate)
////      }).groupBy(_.simId)
////    }
//
//    val x = lines.flatMap(c => sims(c).map(sim => sim match {
//      case (simId, startDate) => CustomerSimStart(customerId(c), simId, startDate)
//    })).groupBy(_.simId)
//      .cache()
//
//    def f(iterable: Iterable[CustomerSimStart]) = {
//      val sorted = iterable.toArray.sortBy(_.startDate)
//      val lastOwner = sorted.last
//      sorted.sliding(2).map{
//        case Array(a,b) => CustomerSimInterval(a.customerId, a.simId, a.startDate, Some(b.startDate))
//      }.toArray ++ Array(CustomerSimInterval(lastOwner.customerId, lastOwner.simId, lastOwner.startDate, None))
//    }
//
//    val y: RDD[CustomerSimInterval] = x.mapValues(f).flatMap(entry => entry._2)
//
//    y.saveAsTextFile(localPath+"""customer_output""")
//
//
//    frame.printSchema()
//
////    val links: RDD[(String, Iterable[String])] = lines.map{ s =>
////      val parts = s.split("\\s+")
////      (parts(0), parts(1))
////    }.distinct().groupByKey().cache()
////    var ranks: RDD[(String, Double)] = links.mapValues(v => 1.0)
////
////    for (i <- 1 to iters) {
////      val x: RDD[(String, (Iterable[String], Double))] = links.join(ranks)
////      val contribs: RDD[(String, Double)] = x.values.flatMap { case (urls, rank) =>
////        val size = urls.size
////        urls.map(url => (url, rank / size))
////      }
////      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
////    }
//
////    val f:PartialFunction[Int, String] = { case 1 => "One" }
////    f(2)
////
////
////    val output = ranks.collect()
////    output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
////
////
////    Thread.sleep(1000*1000)
//    spark.stop()
//  }
//}
//// scalastyle:on println