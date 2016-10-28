package com.scala.main.pageranker

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageRank {

	def main(args: Array[String]) = {
			//Start the Spark context
			val conf = new SparkConf()
			.setAppName("PageRank")
			.setMaster("local")
			val sc = new SparkContext(conf)
			try {
			  
			  parseInputFile (sc)
			  pageRankCalculation ()
					
					sc.stop()
		} catch {
		case e: Exception => e.printStackTrace
		}
	}
	
	def pageRankCalculation () = {
	  
	}
	
	def parseInputFile(sc: SparkContext) = {
	  val pairRDD = sc.textFile("wikipedia-simple-html.bz2")
			.map(line => {
				Bz2WikiParser.parseLine(line)
			})
			.map(node => node)
			.filter(node => node.pageName.length() > 0)
			.map { node => (node.pageName, (node.adjacencyList, node.pageRank)) }
			
			val recordCount = sc.parallelize(pairRDD.keys.collect()).count()
			println(s"count is $recordCount")
			
			//collect might be needed
			val pageRankRDD = pairRDD.map((record) => {
			  val (pageName, (adjacencyList, pageRank)) = record
			  (pageName, (adjacencyList, 1/recordCount.toDouble))
			})
			pageRankRDD.saveAsTextFile("inputFile")

	}
	
}