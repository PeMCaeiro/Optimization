package sparkgenetic.spark

import org.apache.spark.SparkEnv
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions._
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
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import java.nio.file.{Paths, Files}

object PortfolioModule extends SparkSessionWrapper with Logging {
  import spark.implicits._
	val stocksDf = OptimizationModule.stocksDf
		.cache
	val symbolsList = DataModule.symbolsList
	
	// PORTFOLIOS
	
	/*
	 * Creates a random portfolio without constraints dataframe and normalizes its weights
	 */
	def createUnconstrainedRandomPortfolio(sDate: String, windowInt:Int, numAssets:Int): DataFrame={
		val date = DataModule.stringToDate(sDate)
		val string = s"mean_${windowInt}"
		logger.warn("createUnconstrainedRandomPortfolio")
		var sampleDf = stocksDf
		sampleDf = stocksDf
			.filter(col("date") === date)
			.filter(col(string).isNotNull)
			.orderBy(rand)
			.sample(false, 0.25)
			.limit(numAssets)
			.withColumn("weight", rand)
			.sort(desc("weight"))
		sampleDf = DataModule.normalizeWeightsDf(sampleDf)
		sampleDf = DataModule.setCheckpointDf(sampleDf, true)
		sampleDf
	}
	
	/*
	 * Returns the portfolio with highest returns @ the specified date
	 * in the defined rolling window
	 */
	def createBuyAndHoldPortfolio(sDate: String, windowInt: Int, numAssets:Int): DataFrame={
		logger.warn("createBuyAndHoldPortfolio")
		val date = DataModule.stringToDate(sDate)
		if(!DataModule.windowsList.contains(windowInt)){
			throw new IllegalArgumentException("Window value is not one of the allowed values: "+ DataModule.windowsList)
		}
		val string = s"mean_${windowInt}"
		var sampleDf = stocksDf.filter(col("date") === date)
			.filter(col(string).isNotNull)
			.orderBy(col(string).desc)
			.limit(numAssets)
			.withColumn("weight", lit(1.0/numAssets))
			//.cache
			.toDF
		sampleDf = DataModule.setCheckpointDf(sampleDf, true)
		return sampleDf
	}
	
	/*
	 * Returns the portfolio with highest returns @ the specified date
	 * in the defined rolling window
	 */
	def createOptimizedPortfolio(sDate:String, windowInt:Int, numAssets:Int, generations:Int, popSize:Int, mutationRate:Double, fitnessType:String):DataFrame={
		logger.warn("createOptimizedPortfolio")
		val dDate = DataModule.stringToDate(sDate)
		if(!DataModule.windowsList.contains(windowInt)){
			throw new IllegalArgumentException("Window value is not one of the allowed values: "+ DataModule.windowsList)
		}
		var portfolioDf = OptimizationModule.geneticAlgorithm(generations, popSize, mutationRate, windowInt, dDate, numAssets, fitnessType:String)
		return portfolioDf
	}
	
		/*
	 * Get average Portfolio returns
	 */
	def getPortfolioReturn(portfolio:DataFrame, windowInt:Int): Double={
		//logger.warn("getPortfolioReturn")
		val stringCol = s"mean_${windowInt}"
		var portfolioDf = portfolio.withColumn("weightMeanRor", col(stringCol)*col("weight"))
		val df = portfolioDf.agg(sum("weightMeanRor"))
		val list = portfolioDf.agg(sum("weightMeanRor"))
			.map( r => r(0).asInstanceOf[Double] )
			.collect
			.toList
		list(0)
	}
	
	/*
	 * Average correlation of assets in a portfolio according to Tierens and Anadu (2004)
	 * Method (A) with full correlation matrix
	 */
	def getPortfolioVariance(portfolio:DataFrame, windowInt:Int, dDate:Date, numAssets:Int): Double={
		//logger.warn("getPortfolioVariance")
		val stringCol = s"mean_${windowInt}"
		var portfolioDf = portfolio
		var sampleDf = stocksDf
		val leftLimitDate = windowInt match{
			//week
			case 5 => DataModule.subtractDays(dDate, 8)
			//2 weeks
			case 10 => DataModule.subtractDays(dDate, 16)
			//month
			case 20 => DataModule.subtractDays(dDate, 31)
			//quarter
			case 60 => DataModule.subtractDays(dDate, 93)
			//half-year
			case 120 => DataModule.subtractDays(dDate, 183)
			//year
			case 252 => DataModule.subtractDays(dDate, 366)
			case _ => throw new IllegalArgumentException("Window value is not one of the allowed values: "+ DataModule.windowsList)
		}
		val rightLimitDate = windowInt match{
			//week
			case 5 => DataModule.addDays(dDate, 1)
			//2 weeks
			case 10 => DataModule.addDays(dDate, 1)
			//month
			case 20 => DataModule.addDays(dDate, 1)
			//quarter
			case 60 => DataModule.addDays(dDate, 1)
			//half-year
			case 120 => DataModule.addDays(dDate, 1)
			//year
			case 252 => DataModule.addDays(dDate, 1)
			case _ => throw new IllegalArgumentException("Window value is not one of the allowed values: "+ DataModule.windowsList)
		}
		//PREPARE DATA
		val pSymbolsList = portfolioDf.sort(desc("weight"))
			.limit(numAssets)
			.sort(asc("symbol"))
			.select(col("symbol"))
			.map( r => r.getString(0) )
			.collect
			.toList
		val pWeightsList = portfolioDf.sort(desc("weight"))
			.limit(numAssets)
			.sort(asc("symbol"))
			.select(col("weight"))
			.map( r => r.getDouble(0) )
			.collect
			.toList

		sampleDf = sampleDf.filter( (col("date") <= rightLimitDate) && (col("date") >= leftLimitDate) )
		var tempDf = sampleDf.union(portfolioDf)
			.distinct
			.sort(col("date").asc)
		var pivotedRorSymbolDf = tempDf.filter($"symbol" isin (pSymbolsList:_*))
			.groupBy("date")
			.pivot("symbol")
			.agg(first(s"ror_${windowInt}"))
			.sort(asc("date"))
			//.cache
			.toDF
		val datesDf = pivotedRorSymbolDf.na.drop("any").select("date")
		var df = pivotedRorSymbolDf.na.drop("any").drop("date")
		df = DataModule.setCheckpointDf(df, true)
		val rows = new VectorAssembler().setInputCols(df.columns).setOutputCol("corr_features")
      .transform(df)
      .select("corr_features")
      .rdd
   	val items_mllib_vector = rows.map(_.getAs[org.apache.spark.ml.linalg.Vector](0))
    	.map(org.apache.spark.mllib.linalg.Vectors.fromML)
    //val correlMatrix: Matrix = Statistics.corr(items_mllib_vector, "pearson") -> for correlation instead of covariance
   	val correlMatrix = new RowMatrix(items_mllib_vector).computeCovariance
   	val pairwiseArr = new ListBuffer[Array[Double]]()
   	//Get result into local array
		for( i <- 0 to correlMatrix.numRows-1){
		  for(j <- 0 to correlMatrix.numCols-1){
		    pairwiseArr += Array(i, j, correlMatrix.apply(i,j))
		  }
		}
		val pairwiseDf = pairwiseArr.map(x => pairRow(x(0), x(1), x(2))).toDF()
		var outerSumUp = 0.0
		var outerSumDown = 0.0
		var outerSum = 0.0
		(0 to numAssets-1).par.foreach{
			i =>
				var innerSum = 0.0
				for(j <- 0 to numAssets-1){
					val cov= pairwiseDf.filter( (col("i") === i) && (col("j") === j) )
						.select("corr")
						.first
						.getDouble(0)
					innerSum = innerSum + (cov*pWeightsList(i)*pWeightsList(j))
				}
				outerSum = outerSum+innerSum
		}
		outerSum
	}
	
	/*
	 * Gets the portfolio value at sDate, taking budget allocation
	 * and transaction costs into account
	 */
	def getPortfolioValue(portfolio:DataFrame, sDate:String, iBudget:Int, transaction:Int, numAssets:Int):Double={
		val portfolioShares = getPortfolioWithShares(portfolio, sDate, iBudget)
		val value = getPortfolioSharesValue(portfolioShares, sDate, numAssets)
		val v = value - transaction*numAssets
		return v
	}
	
	/*
	 * Indicates the ROI starting at sStartDate and ending on sEndDate
	 * Takes into account transaction costs and budget
	 */
	def getPortfolioROI(portfolio:DataFrame, sStartDate:String, sEndDate:String, iBudget:Int, transaction:Int, numAssets:Int):Double={
		logger.warn("getPortfolioROI")
		val sharePortfolio = PortfolioModule.getPortfolioWithShares(portfolio, sStartDate, iBudget)
		val startValue = PortfolioModule.getPortfolioSharesValue(sharePortfolio, sStartDate, numAssets) - transaction*numAssets
		//logger.warn(startValue)
		val endValue = PortfolioModule.getPortfolioSharesValue(sharePortfolio, sEndDate, numAssets)
		//logger.warn(endValue)
		val roi = (endValue - startValue)/startValue
		return roi
	}
	
	/*
	 * Returns, from a portfolio with shares column,
	 * its value in EUR at sDate
	 */
	def getPortfolioSharesValue(portfolioShares:DataFrame, sDate:String, numAssets:Int):Double={
		val dDate = DataModule.stringToDate(sDate)
		//get conversion rate of EUR to USD in that date
		logger.warn("portfolio value @ date: "+sDate)
		val pSymbolsList = portfolioShares.sort(desc("weight"))
			.limit(numAssets)
			.select(col("symbol"))
			.map( r => r.getString(0) )
			.collect
			.toList
		val pWeightsList = portfolioShares.sort(desc("weight"))
			.limit(numAssets)
			.select(col("weight"))
			.map( r => r.getDouble(0) )
			.collect
			.toList
		val pCurrencyList = portfolioShares.sort(desc("weight"))
			.limit(numAssets)
			.select(col("currency"))
			.map( r => r.getString(0) )
			.collect
			.toList
		var sum = 0.0
		(0 until pSymbolsList.size).foreach{
			i =>
				var ddDate = dDate
				val symbol = pSymbolsList(i)
				val weight = pWeightsList(i)
				val currency = pCurrencyList(i)
				var symbolPrice = 0.0
				var nonExistant = true
				while(nonExistant == true){
					try{
					symbolPrice = stocksDf.filter($"date"===ddDate)
						.filter(col("symbol")===symbol)
						.select(col("adjClose"))
						.first
						.getDouble(0)
					nonExistant = false
					}catch{
						case _ : Throwable =>
							nonExistant = true
							ddDate = DataModule.subtractDays(ddDate, 1)
					}
				}
				val shares = portfolioShares
					.filter(col("symbol")===symbol)
					.select(col("shares"))
					.first
					.getInt(0)
				var xRate = DataModule.currencyDf
					.filter($"date"===DataModule.dateToString(ddDate))
					.select(col("adjClose"))
					.map( r => r.getDouble(0) )
					.collect
					.toList
				val rate = xRate(0)
				if(currency == "EUR"){
					sum = sum + (shares*symbolPrice)
				}else{
					sum = sum + ((symbolPrice*rate)*shares)
				}
		}
		return sum
	}
	
	/*
	 * Adds a shares column to the portfolio individual DataFrame representation
	 */
	def getPortfolioWithShares(portfolio:DataFrame, sDate:String, iBudget:Int):DataFrame={
		val date = DataModule.stringToDate(sDate)
		var portfolioDf = portfolio.withColumn("sharesTemp", (col("weight")*iBudget)/col("adjClose") )
			.withColumn("shares", col("sharesTemp").cast("int"))//.cast("IntegerType"))
			.drop("sharesTemp")
		portfolioDf = DataModule.setCheckpointDf(portfolioDf, true)
		//logger.warn(portfolioDf.show)
		return portfolioDf
	}
}