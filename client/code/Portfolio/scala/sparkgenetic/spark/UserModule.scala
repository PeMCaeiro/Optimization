package sparkgenetic.spark

import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions._
//import org.apache.spark.sql.Encoders
//import org.apache.spark.sql.SQLImplicits
import scala.util.Random
import org.apache.spark.sql.types.StructType
import scala.collection.mutable._
import org.apache.spark.sql.functions._
import java.sql.Date
import java.util.Calendar
import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import java.nio.file.{Paths, Files}
import java.nio.file.{Paths, Files}

object UserModule extends Logging with SparkSessionWrapper{
  //var portfolio = spark.emptyDataFrame
	/*Contact with PortfolioModule*/
	
	def getOptimizedPortfolio(sDate:String, windowInt:Int, budget:Int, numAssets:Int, transaction:Double, generations:Int, popSize:Int, mutationRate:Double, fitnessType:String):DataFrame={
		val portfolio = PortfolioModule.createOptimizedPortfolio(sDate, windowInt, numAssets, generations, popSize, mutationRate, fitnessType)
		portfolio
	}
  
  def getPortfolioAllocation(portfolio:DataFrame, sDate:String, iBudget:Int):DataFrame={
  	//with shares
  	val df = PortfolioModule.getPortfolioWithShares(portfolio:DataFrame, sDate:String, iBudget:Int)
  	println(df.show)
  	df
  }
  
  def getPortfolioRoi(portfolio:DataFrame, sStartDate:String, sEndDate:String, iBudget:Int, transaction:Int, numAssets:Int):Double={
  	//roi
  	val roi = PortfolioModule.getPortfolioROI(portfolio:DataFrame, sStartDate:String, sEndDate:String, iBudget:Int, transaction:Int, numAssets:Int)
  	println("Portfolio ROI @ "+sEndDate+": "+roi)
  	roi
  }
  
  def getPortfolioSummary(portfolio:DataFrame, windowInt:Int, sDate:String, iBudget:Int, transaction:Int, numAssets:Int){
  	val dDate = DataModule.stringToDate(sDate)
  	//variance
  	val variance = PortfolioModule.getPortfolioVariance(portfolio, windowInt, dDate, numAssets)
  	//return
  	val ret = PortfolioModule.getPortfolioReturn(portfolio, windowInt)
  	//get real portfolio value: with allocation
  	val value = PortfolioModule.getPortfolioValue(portfolio, sDate, iBudget, transaction, numAssets)
  	println("Portfolio Summary @ "+sDate+": ")
  	println("	Return: "+ret)
  	println("	Variance: "+variance)
  	println("	Value: "+value)
  }
	
	/*
	 * Tests 
	 */
	
	def main(args: Array[String]):Unit={
		val cardinality = 20
		val numAssets = cardinality
		var iWindow = 120
		val iBudget = 100000
		val transaction = 8
		val generations = 100
		val popSize = 32
		val windowInt = iWindow
		val mutationRate = 0.03
		val fitnessType = "linear"
		var x = 0.0
		
		//Bullish: 2017-01-03 to 2018-01-02
		var sDate = "2017-01-03"
		var endDate = "2018-01-02"
		iWindow = 252
		logger.warn("---------BULLISH1--------------")
		/*var r = PortfolioModule.createUnconstrainedRandomPortfolio(sDate, iWindow, cardinality)
		x = getPortfolioRoi(r, sDate, "2017-01-10", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2017-01-17", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2017-02-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2017-04-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2017-07-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2017-10-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-01-03", iBudget, transaction, numAssets)
		getPortfolioSummary(r, windowInt, sDate, iBudget, transaction, numAssets)
		var y = PortfolioModule.createBuyAndHoldPortfolio(sDate, iWindow, cardinality)
		x = getPortfolioRoi(y, sDate, "2017-01-10", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2017-01-17", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2017-02-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2017-04-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2017-07-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2017-10-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2018-01-03", iBudget, transaction, numAssets)
		getPortfolioSummary(y, windowInt, sDate, iBudget, transaction, numAssets)
		* 
		*/
		var p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2017-01-10", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-01-17", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-02-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-04-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-07-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-10-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-03", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		
		p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2017-01-10", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-01-17", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-02-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-04-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-07-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-10-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-03", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		
		p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2017-01-10", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-01-17", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-02-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-04-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-07-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-10-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-03", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		
		p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2017-01-10", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-01-17", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-02-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-04-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-07-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-10-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-03", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		
		p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2017-01-10", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-01-17", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-02-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-04-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-07-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2017-10-03", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-03", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		
		//Sideways: 2018-01-02 to 2018-06-01
		sDate = "2018-01-02"
		iWindow = 120
		/*logger.warn("---------SIDEWAYS1--------------")
		var r = PortfolioModule.createUnconstrainedRandomPortfolio(sDate, iWindow, cardinality)
		x = getPortfolioRoi(r, sDate, "2018-01-09", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-02-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-06-28", iBudget, transaction, numAssets)
		getPortfolioSummary(r, windowInt, sDate, iBudget, transaction, numAssets)
		var y = PortfolioModule.createBuyAndHoldPortfolio(sDate, iWindow, cardinality)
		getPortfolioSummary(y, windowInt, sDate, iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2018-01-09", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2018-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2018-02-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2018-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-06-28", iBudget, transaction, numAssets)
		
		var p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2018-01-09", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-02-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-06-28", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		
		p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2018-01-09", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-02-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-06-28", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		
		p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2018-01-09", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-02-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-06-28", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		
		p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2018-01-09", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-02-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-06-28", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		
		p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2018-01-09", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-02-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2018-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2018-06-28", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		*/
		//Bearish: 2008-01-02 to 2009-01-02
		/*logger.warn("---------BEARISH1--------------")
		 p = getOptimizedPortfolio(sDate, windowInt, iBudget, numAssets, transaction, generations, popSize, mutationRate, fitnessType)
		x = getPortfolioRoi(p, sDate, "2008-01-09", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2008-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2008-02-01", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2008-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2008-07-02", iBudget, transaction, numAssets) 
		x = getPortfolioRoi(p, sDate, "2008-10-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(p, sDate, "2009-01-02", iBudget, transaction, numAssets)
		getPortfolioSummary(p, windowInt, sDate, iBudget, transaction, numAssets)
		r = PortfolioModule.createUnconstrainedRandomPortfolio(sDate, iWindow, cardinality)
		x = getPortfolioRoi(r, sDate, "2008-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2008-02-01", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2008-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2008-07-02", iBudget, transaction, numAssets) 
		x = getPortfolioRoi(r, sDate, "2008-10-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(r, sDate, "2009-01-02", iBudget, transaction, numAssets)
		getPortfolioSummary(r, windowInt, sDate, iBudget, transaction, numAssets)
		y = PortfolioModule.createBuyAndHoldPortfolio(sDate, iWindow, cardinality)
		x = getPortfolioRoi(y, sDate, "2008-01-16", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2008-02-01", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2008-04-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2008-07-02", iBudget, transaction, numAssets) 
		x = getPortfolioRoi(y, sDate, "2008-10-02", iBudget, transaction, numAssets)
		x = getPortfolioRoi(y, sDate, "2009-01-02", iBudget, transaction, numAssets)
		getPortfolioSummary(y, windowInt, sDate, iBudget, transaction, numAssets)*/
		
		
		println("Press any key to end Spark and stop access to Spark WebUI")
		System.in.read()
		spark.stop()
		
  }
	
}