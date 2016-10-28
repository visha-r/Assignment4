package com.scala.main.pageranker

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object temperatureAvgByKey {
	def main(args: Array[String]) = {

		//Start the Spark context
		val conf = new SparkConf()
		.setAppName("temperatureAvgByKey")
		.setMaster("local")
		val sc = new SparkContext(conf)
		val pairRDD = sc.textFile("input1.txt")
		.map(line => {
		  line.split(",")})
		.filter(fields => "TMAX".equals(fields(2)))
		.map(fields => (fields(0), Integer.parseInt(fields(3))))
		val sumByKey = pairRDD.reduceByKey((temp, sum) => temp + sum)
		val countByKey = pairRDD.foldByKey(0)((temp, sum) => temp + 1)
		val sumCountByKey = sumByKey.join(countByKey)
		val avgByKey = sumCountByKey.map(t => (t._1, t._2._1 / t._2._2.toDouble))
		avgByKey.collect.map(println)
		sc.stop()
	}
}