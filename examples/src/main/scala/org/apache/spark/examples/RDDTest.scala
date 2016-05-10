/*
 * this is my own file to learn spark
 *
*/
package org.apache.spark.examples

import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkContext, SparkConf}

object RDDTest {
  def main(args: Array[String]) {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getSimpleName)

    val sc = new SparkContext(conf)
    val hadoopRDD = sc.textFile("hdfs://hmaster:9000/zh/data/sougou/SogouQ.mini", 4)
      .flatMap(x => x.split("\\s"))

    val rdd1 = sc.parallelize(Array(1, 2, 3, 4), 4).map(x => (x, 1))
    val rdd2 = sc.parallelize(Array(2, 3, 4, 5), 4).map(x => (x, 1))

    // scalastyle:off println
    // wordCount.foreach(println)
    println("*********************************")

    var beg = System.currentTimeMillis()
    val cogroup1 = rdd1.join(rdd2, 4).count()

    val period_1 = System.currentTimeMillis() - beg

    beg = System.currentTimeMillis()
    val rdd3 = rdd1.repartition(4)
    val rdd4 = rdd1.repartition(4)
    val cogroup2 = rdd3.join(rdd4, 4).count()
    val period_2 = System.currentTimeMillis() - beg
    // val groupBy1 = rdd1.groupByKey(4)
    println(period_2 - period_1 + "***********************************")
    // println(cogroup1.partitions.length)
    // println(groupBy1.partitions.length)


    // scalastyle:on println


  }
}
