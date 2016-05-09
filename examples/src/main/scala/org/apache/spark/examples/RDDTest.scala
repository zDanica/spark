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

    val cogroup1 = rdd1.cogroup(rdd1, 4)
    val groupBy1 = rdd1.groupByKey(4)

    println(cogroup1.partitions.length)
    println(groupBy1.partitions.length)


    // scalastyle:on println


  }
}
