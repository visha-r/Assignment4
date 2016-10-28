package com.scala.main.pageranker

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object temperatureAvgByKeyExt {
	def main(args: Array[String]) = {
		val conf = new SparkConf()
		.setAppName("temperatureAvgByKey")
		.setMaster("local")
		val sc = new SparkContext(conf)
		val pairRDD = sc.textFile("input1.txt")
		.map(line => line.split(","))
		.filter(fields => "TMAX".equals(fields(2)))
		.map(fields => (fields(0), Integer.parseInt(fields(3))))

		val temperatureCombiner = pairRDD.combineByKey(
				(temperature:Int) => {(1, temperature)}, 
				(acc: (Int, Int), temperature) => {(acc._1 + 1, acc._2 + temperature)},
				(acc1: (Int, Int), acc2:(Int, Int)) => {(acc1._1+acc2._1, acc1._2+acc2._2)})

				val averageTemperature = temperatureCombiner.mapValues({
					acc => acc._2/acc._1.toFloat
				})

				averageTemperature.foreach((record) => {
					val(stationId,average) = record
							println(stationId+ " - " + average)
				})

				//averageTemperature.collect().foreach(println)
				sc.stop()

	}
}