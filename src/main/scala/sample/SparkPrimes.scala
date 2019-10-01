package sample

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.math.random

/** Computes an approximation to pi */
object SparkPrimes {
  def isPrime(n:Int):Boolean = n match {
    case 1 => false
    case 2 => true
    case x if x % 2 == 0 => false
    case _ => !(3 to (n/2, 2)).exists(n % _ == 0)
  }
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Spark Odds")
      .master("local[2]")
      .getOrCreate()
    val ex: RDD[Int] = spark.sparkContext.parallelize(1 until 1000000, 4)
      .filter(isPrime(_))
    println("Created ")
    val count = ex.count()
    println(s"Found $count primes")
    spark.stop()
  }
}
