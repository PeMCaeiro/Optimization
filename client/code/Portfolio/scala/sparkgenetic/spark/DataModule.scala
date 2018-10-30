package sparkgenetic.spark

import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions._
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.Encoders
//import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.types.StructType
import scala.collection.mutable._
import org.apache.spark.sql.functions._
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File
import org.apache.commons.io.FileUtils

case class Stock(date:Date, open:Double, high:Double, low:Double,
			close:Double, adjClose:Double, volume:Int, symbol:String, index:String, currency:String)
			
case class Currency(date:Date, open:Double, high:Double, low:Double,
			close:Double, adjClose:Double, volume:Int, from:String, to:String)
	

object DataModule extends SparkSessionWrapper with Logging{
	import spark.implicits._
	import org.apache.spark.sql.catalyst.ScalaReflection
	
	val stockSchema = ScalaReflection.schemaFor[Stock].dataType.asInstanceOf[StructType]
	val currencySchema = ScalaReflection.schemaFor[Currency].dataType.asInstanceOf[StructType]
	//Define a list of windowIntervals
	val windowsList = List(5, 10, 20, 60, 120, 252)
	
	lazy val symbolsList = {
		getDistinctSymbols(stocksDf)
	}
	val symbolsSize = symbolsList.size
	
	lazy val currencyDf: DataFrame = {
		var df = loadCurrenciesDf(None)
			//.cache()
			.toDF
		//logger.warn("Dropping excess columns and switching to and from")
		df = df.drop("open").drop("close")
			.drop("high").drop("low")
			.drop("volume")
			//.cache()
		//logger.warn("Adding day, month and year columns")
		df =  df.withColumn("year", year(col("date")))
			.withColumn("month", month(col("date")))
			.withColumn("day", dayofmonth(col("date")))
			//.cache()
		//logger.warn("Sorting by date")
		df = df.sort(asc("date")).cache()
		df = setCheckpointDf(df, true)
		df
	}
	
	//Lazy initiate the Stocks DataFrame
	lazy val stocksDf = {
		try{
			var df = loadParquetToDf("assets")
				.cache
			df
		}catch{
			case _: Throwable =>
				logger.warn("Load assets from CSV files")
				var df = loadAssetsDf(None)
					//.cache()
				logger.warn(df.show)
				logger.warn("Save assets DF to parquet file")
				saveDfToParquet(df, "temp")
				df = loadParquetToDf("temp")
					.cache
				
				
				logger.warn("Replace symbol x.y to x_y - SPARK compatibility - . reserved for structs")
				df = df.withColumn("ticker", translate(col("symbol"), ".", "_"))
					.drop("symbol")
					.withColumnRenamed("ticker","symbol")
					//.cache()
				
				logger.warn("Dropping excess columns")
				df = df.drop("open").drop("close").drop("high").drop("low")//.cache()
				//remaining attributes: date adjClose volume symbol currency
				/*
				logger.warn("Separate symbol and index into different columns")
				df = df.withColumn("symbol", split(col("symbol_index"), "_").getItem(0))
					.withColumn("index", split(col("symbol_index"), "_").getItem(1))
					.drop("symbol_index")
				*/
				logger.warn("Sorting by symbol & date")
				df = df.sort(asc("symbol"), asc("date")).cache()
				
				logger.warn("Adding day, month and year columns")
				df =  df.withColumn("year", year(col("date")))
					.withColumn("month", month(col("date")))
					.withColumn("day", dayofmonth(col("date")))
					//.cache()
					
				logger.warn("Adding adjCloseLag_x and ror_x columns")
				val w: WindowSpec = Window.partitionBy("symbol")
					.orderBy(col("date"))
				for(i <- windowsList){
					logger.warn(s"... where x = $i")
					df = df.withColumn(s"adjCloseLag_$i", lag($"adjClose", i).over(w))
						.withColumn(s"ror_$i", ($"adjClose" - col(s"adjCloseLag_$i")) / col(s"adjCloseLag_$i") * 100 )
						.drop(s"adjCloseLag_$i")
				}
				
				logger.warn("Adding sampleStddev_x wrt ror_x columns")
				for(i <- windowsList){
					logger.warn(s"... where x = $i")
					var tempW: WindowSpec = Window.partitionBy("symbol")
						.orderBy(col("date"))
						.rowsBetween(-i, 0)
					df = df.withColumn( s"sampleStddev_$i", stddev(col(s"ror_$i")).over(tempW) )
				}
				
				logger.warn("Adding mean_x wrt ror_x columns")
				for(i <- windowsList){
					logger.warn(s"... where x = $i")
					var tempW: WindowSpec = Window.partitionBy("symbol")
						.orderBy(col("date"))
						.rowsBetween(-i, 0)
					df = df.withColumn( s"mean_$i", mean(col(s"ror_$i")).over(tempW) )
				}
				
				//df = setCheckpointDf(df, true)
				//logger.warn(df.show)
				logger.warn("RDD partitions")
				logger.warn(df.rdd.partitions.size)
				logger.warn("Saving ETL result to Parquet file")
				saveDfToParquet(df, "assets")
				df
		}
		
		/*
		 * 
		//Test -> sum the columns in the same row
		t1 = t1.withColumn( s"lagRorDifMeanSUM_${i}", round(lagRorV.map(c => col(c)).reduceLeft((c1,c2) => c1+c2)))
		logger.warn("Create all lagRorDifMean_$window_$countToWindow_$symbol")
		tempW = Window.partitionBy("date")
			.orderBy("symbol")
			.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
		*/
		
		//logger.warn("Drop rows with null values")
		//t1 = t1.na.drop().cache()
		//t1.show()
	}
	
	//FUNCTIONS TO SAVE AND LOAD DFs TO PARQUET
	
	def saveDfToParquet(df: DataFrame, fileName: String): Unit ={
		logger.warn("Saving DF to "+fileName+" Parquet file")
		val hdfsHost: String = "quickstart.cloudera"
		df.write
			.format("parquet")
			//.save("hdfs://"+hdfsHost+":8020/user/cloudera/"+fileName+".parquet")
			.save("file:///home/osiris_wrath/Code/hdfs/import_data/"+fileName+".parquet")
	}
	
	def loadParquetToDf(fileName: String): DataFrame ={
		logger.warn("Loading "+fileName+" to DF")
		val hdfsHost: String = "quickstart.cloudera"
		var df = spark.read
			.format("parquet")
			.option("header", "true") //reading the headers
			.option("mode", "DROPMALFORMED")
			.option("delimiter",";")
			.option("dateFormat", "yyyy-MM-dd")
			//.load("hdfs://"+hdfsHost+":8020/user/cloudera/"+fileName+".parquet")
			.load("file:///home/osiris_wrath/Code/hdfs/import_data/"+fileName+".parquet/*")
			df
	}
	
	//FUNCTIONS TO LOAD FROM CSVs
	
	def loadAssetsDf(hdfsPath:Option[String]): DataFrame = {
		logger.warn("Loading stocks CSV files to DataFrame")
		val hdfsHost: String = "quickstart.cloudera"
			
		hdfsPath match{
			case None =>
				val df: DataFrame = spark.read
					.format("csv")
		   		.option("header", "true") //reading the headers
		   		.option("mode", "DROPMALFORMED")
		   		.option("delimiter",";")
		   		.option("dateFormat", "yyyy-MM-dd")
		   		.schema(stockSchema)
		   		//.load("hdfs://"+hdfsHost+":8020/user/cloudera/data/*.csv")
		   		.load("file:///home/osiris_wrath/Code/hdfs/import_data/data/*.csv")
				return df
			case Some(x) =>
				val df: DataFrame = spark.read
					.format("csv")
		   		.option("header", "true") //reading the headers
		   		.option("mode", "DROPMALFORMED")
		   		.option("delimiter",";")
		   		.option("dateFormat", "yyyy-MM-dd")
		   		.schema(stockSchema)
		   		.load(hdfsPath.get)
		   	return df
		}
	}
	
		def loadCurrenciesDf(hdfsPath:Option[String]): DataFrame = {
		logger.warn("Loading currency CSV files to DataFrame")
		val hdfsHost: String = "quickstart.cloudera"
			
		hdfsPath match{
			case None =>
				val df: DataFrame = spark.read
					.format("csv")
		   		.option("header", "true") //reading the headers
		   		.option("mode", "DROPMALFORMED")
		   		.option("delimiter",";")
		   		.option("dateFormat", "yyyy-MM-dd")
		   		.schema(currencySchema)
		   		//.load("hdfs://"+hdfsHost+":8020/user/cloudera/data/currency/*.csv")
		   		.load("file:///home/osiris_wrath/Code/hdfs/import_data/data/currency/*.csv")
				return df
			case Some(x) =>
				val df: DataFrame = spark.read
					.format("csv")
		   		.option("header", "true") //reading the headers
		   		.option("mode", "DROPMALFORMED")
		   		.option("delimiter",";")
		   		.option("dateFormat", "yyyy-MM-dd")
		   		.schema(currencySchema)
		   		.load(hdfsPath.get)
		   	return df
		}
	}
	
	//AUX FUNCTIONS
	
	def changeNumberRange(number:Double, oldMin:Double, oldMax:Double, newMin:Double, newMax:Double):Double={
		val oldRange = oldMax-oldMin
		if (oldRange == 0){
			return newMin
		}
		else{
			val newRange = newMax-newMin
			val newValue = ( ((number-oldMin)*newRange) / oldRange ) + newMin 
			return newValue
		}
	}
	
	def daysToSecs(i: Int): Int={
		//Hive timestamp is interpreted as UNIX timestamp in seconds
		val x: Int = i * 86400
		return x
	}
	
	/*
	 * Obtain the date after x days
	 */
	def addDays(date:java.sql.Date , days:Int):java.sql.Date={
		//logger.warn("addDays")
		val cal:Calendar = Calendar.getInstance
		val utilDate = dateSqlToUtil(date)
    cal.setTime(date)
    cal.add(Calendar.DATE, days) //minus number would decrement the days
    val sqlDate = dateUtilToSql(cal.getTime)
    return sqlDate
  }
	
	/*
	 * Obtain the date before x days
	 */
	def subtractDays(date:java.sql.Date , days:Int):java.sql.Date={
		val cal:Calendar = Calendar.getInstance
		val utilDate = dateSqlToUtil(date)
    cal.setTime(date)
    val negativeDays = -days
    cal.add(Calendar.DATE, negativeDays) //minus number would decrement the days
    val sqlDate = dateUtilToSql(cal.getTime)
    return sqlDate
  }
	
	/*
	 * Convert java.util.Date to java.sql.Date
	 */
	def dateUtilToSql(date:java.util.Date):java.sql.Date={
		val month = date.getMonth
		val year = date.getYear
		val day = date.getDay
		val sqlDate = new java.sql.Date(year, month, day)
		return sqlDate
	}
	
	/*
	 * Convert java.sql.Date to java.util.Date
	 */
	def dateSqlToUtil(date:java.sql.Date):java.util.Date={
		val month = date.getMonth
		val year = date.getYear
		val day = date.getDay
		val utilDate = new java.util.Date(year, month, day)
		return utilDate
	}
	
	def stringToDate(s: String): Date={
		//logger.warn("Convert "+s+" to date")
		val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
		val d: java.util.Date = format.parse(s)
		val date: java.sql.Date = new java.sql.Date(d.getTime)
		return date
	}
	
	def dateToString(date:java.sql.Date):String={
		val format:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
		val text:String = format.format(date)
		return text
		
	}
	
	def getDistinctSymbols(df: DataFrame): List[String]={
		//Da erro em modo YARN
		logger.warn("Get distinct symbols")
		val distinctSymbols = df.select("symbol")
			.distinct
			.map( r => r.getString(0) )
			.collect
			.toList
			.sorted
		return distinctSymbols
	}
	
	def getDistinctDates(df: DataFrame): List[Date]={
		logger.warn("Get distinct dates")
		val sDistinctDates = df.select("date")
			.distinct
			.map( r => r(0).asInstanceOf[Date] )
			.collect
			.toList
			//.sorted
		val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
		val distinctDates = sDistinctDates //sDistinctDates.map(d => format.parse(d))
		val distinctDatesCount = distinctDates.size
		return distinctDates
	}
	
	def setCheckpointDf(df: DataFrame, b: Boolean): DataFrame={
		//logger.warn("Checkpoint")
		//spark.sparkContext.setCheckpointDir("/tmp")
		spark.sparkContext.setCheckpointDir("file:///home/osiris_wrath/Code/hdfs/checkpoint")
		//logger.warn(df.explain)
		var d = df.checkpoint(b)
		//logger.warn(d.explain)
		//logger.warn("Checkpoint ended")
		return d
	}
	
	def normalizeWeightsDf(df: DataFrame): DataFrame={
		//val rowList = sampleDf.collect().toList.map(e => e.toString())
		val soma = df.agg(sum("weight"))
			.map( r => r(0).asInstanceOf[Double] )
			.collect
			.toList
		val normalizacao = col("temp")/lit(soma(0))
		var d = df.withColumnRenamed("weight", "temp")
			.withColumn("weight", normalizacao)
			.drop("temp")
			//.cache
			.toDF
		return d
		
	}
}