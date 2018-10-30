package sparkgenetic.spark

import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
//import org.apache.spark.sql.Encoders
//import org.apache.spark.sql.SQLImplicits
import java.util.concurrent.atomic.AtomicLong
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
import java.util.concurrent._


case class pairRow(i: Double, j: Double, corr: Double)

object OptimizationModule extends SparkSessionWrapper with Logging{
	import spark.implicits._
	val stocksDf = DataModule.stocksDf.withColumn("weight", lit(0.0))
		.cache
	val symbolsList = DataModule.symbolsList
	var numAssets = 20
	val floor = 0.01
	val ceiling = 0.25
	
	import org.apache.spark.sql.catalyst.ScalaReflection
	//val portfolioSchema = ScalaReflection.schemaFor[Portfolio].dataType.asInstanceOf[StructType]
	
	
	// EVALUATION (FITNESS) FUNCTIONS
	
	def evaluatePortfolioReturn(portfolioDf:DataFrame, windowInt:Int, dDate:Date):Double={
		//logger.warn("evaluatePortfolioReturn")
		val ret = PortfolioModule.getPortfolioReturn(portfolioDf, windowInt)
		//Max Return and Min Variance
		val fitness = ret
		//logger.warn(fitness)
		fitness
	}
	
	def evaluatePortfolioLinearCombination(portfolioDf:DataFrame, windowInt:Int, dDate:Date, returnWeight:Double, riskWeight:Double):Double={
		//logger.warn("evaluatePortfolioLinearCombination " + returnWeight + " , " + riskWeight)
		val ret = PortfolioModule.getPortfolioReturn(portfolioDf, windowInt)
		val risk = PortfolioModule.getPortfolioVariance(portfolioDf, windowInt, dDate, numAssets)
		//Max Return and Min Variance
		val fitness = ret*returnWeight - (risk*riskWeight)
		//logger.warn(fitness)
		fitness
	}
	
	def initRandomPop(dDate:Date, windowInt:Int, popSize:Int):LinkedHashMap[DataFrame,Double]={
		var mapBuf = scala.collection.mutable.LinkedHashMap.empty[DataFrame, Double]
		//Random Initialization of 1st generation
		(0 until popSize).par.foreach{
			x =>
				//logger.warn("Random Individual "+ x)
				val p = createRandomPortfolio(dDate.toString, windowInt)
				mapBuf = this.synchronized{
					mapBuf += (p -> 0.0)
					mapBuf
				}
		}
		return mapBuf
	}
	
	def checkpointPop(map:LinkedHashMap[DataFrame,Double]):LinkedHashMap[DataFrame,Double]={
		var mapBuf = scala.collection.mutable.LinkedHashMap.empty[DataFrame, Double]
		var newMapBuf = scala.collection.mutable.LinkedHashMap.empty[DataFrame, Double]
		//logger.warn("checkpointing generation to cut RDD lineage")
		//logger.warn(mapBuf.size)
		mapBuf = map
		mapBuf.par.foreach{
			case(k:DataFrame,v:Double) =>
				//logger.warn(k.explain)
				val newK = DataModule.setCheckpointDf(k, true)
				//logger.warn(newK.explain)
				newMapBuf = this.synchronized{
					newMapBuf += (newK -> v)
					newMapBuf
				}
				//logger.warn(k.show)
		}
		mapBuf = newMapBuf
		//sort population low to high by value
		mapBuf = scala.collection.mutable.LinkedHashMap(mapBuf.toSeq.sortWith(_._2 > _._2):_*)
		mapBuf
	}
	
	def evaluatePop(fitnessType:String, windowInt:Int, dDate:Date, map:LinkedHashMap[DataFrame,Double]):LinkedHashMap[DataFrame,Double]={
		var mapBuf = map
		fitnessType match{
			case "return" =>
				//logger.warn("evaluate return fitness begin")
				mapBuf.par.foreach{
					case(k:DataFrame,v:Double) =>
						val fitness = evaluatePortfolioReturn/*LinearCombination*/(k, windowInt, dDate/*, 0.5, 0.5*/)
						mapBuf = this.synchronized{
							mapBuf.update(k, fitness)
							mapBuf
						}
						logger.warn(k + "  " + mapBuf.get(k))
						//logger.warn(k.show)
				}
				//sort population low to high by value
				mapBuf = scala.collection.mutable.LinkedHashMap(mapBuf.toSeq.sortWith(_._2 > _._2):_*)
				mapBuf
			case "linear" =>
				logger.warn("evaluate linear fitness begin")
				mapBuf.par.foreach{
					case(k:DataFrame,v:Double) =>
						val fitness = evaluatePortfolioLinearCombination(k, windowInt, dDate, 0.75, 0.25)
						mapBuf = this.synchronized{
							mapBuf.update(k, fitness)
							mapBuf
						}
				}
				//sort population low to high by value
				mapBuf = scala.collection.mutable.LinkedHashMap(mapBuf.toSeq.sortWith(_._2 > _._2):_*)
				mapBuf
			case _ =>
				map
		}
	}
	
	def selectionParents(popSize:Int, map:LinkedHashMap[DataFrame,Double], threshold:Double):LinkedHashMap[DataFrame,Double]={
		//logger.warn("Truncation Parent Selection using threshold")
		//parent truncation selection using threshold idx
		val selection:Int = popSize - (popSize*threshold).toInt
		//logger.warn("Selection "+selection)
		var mapBufKeep = map.dropRight(selection)
		var mapBufDrop = map.drop(selection)
		//logger.warn(mapBufKeep.size)
		//logger.warn(mapBufDrop.size)
		mapBufKeep
	}
	
	def selectionCrossover(popSize:Int, map:LinkedHashMap[DataFrame,Double], threshold:Double, fitnessType:String):List[DataFrame]={
		//logger.warn("Roulette Wheel Crossover Selection")
		//parent truncation selection using threshold idx
		val selection:Int = popSize - (popSize*threshold).toInt
		val parentsPair = scala.collection.mutable.ListBuffer.empty[DataFrame]
		var r = scala.util.Random
		var values = ListBuffer.empty[Double]
		var mapBufKeep = map
		if(fitnessType == "linear"){
			var h = 0
			mapBufKeep.values.par.foreach{
				x =>
					values = this.synchronized{
						values += DataModule.changeNumberRange(mapBufKeep.values.toList(h), -10000, 10000, 1, 10000)
						h = h+1
						values
					}
			}
		}
		//Sum of all chromosomes fitness
		var valuesSum = map.values.sum
		if(fitnessType == "linear"){
			valuesSum = values.sum
		}
		//get parent pair for crossover
		var pA:DataFrame = spark.emptyDataFrame
		var pB:DataFrame = spark.emptyDataFrame
		
		//choose parent A
		var d = r.nextDouble()
		//change random double range from [0.0,1.0] to [0,sum of all fitness values]
		var crossoverPoint = DataModule.changeNumberRange(d, 0.0, 1.0, 0.0, valuesSum)
		//logger.warn("crossover point "+ crossoverPoint)
		var cumulative = 0.0
		var found = false
		(0 until mapBufKeep.values.size).foreach{
			x =>
				//logger.warn("cumulative point "+ cumulative)
				if(fitnessType == "linear")
					cumulative = cumulative + values.toList(x)
				else
					cumulative = cumulative + mapBufKeep.values.toList(x)
				
				if(cumulative >= crossoverPoint && found==false){
					logger.warn(x)
					found = true
					//logger.warn("found cumulative point "+ cumulative)
					pA = mapBufKeep.keys.toList(x)
				}
		}
		
		//choose parent B
		var same = false
		d = r.nextDouble()
		crossoverPoint = DataModule.changeNumberRange(d, 0.0, 1.0, 0.0, valuesSum)
		cumulative = 0.0
		found = false
		logger.warn("Double "+crossoverPoint)
		(0 until mapBufKeep.values.size).foreach{
			x =>
				if(fitnessType == "linear")
					cumulative = cumulative + values.toList(x)
				else
					cumulative = cumulative + mapBufKeep.values.toList(x)
				if(cumulative >= crossoverPoint && found==false){
					logger.warn(x)
					found = true
					//logger.warn("found cumulative point "+ cumulative)
					pB = mapBufKeep.keys.toList(x)
				}
		}

		if(pA == pB ){
			same = true
		}
		//If the same chromosome was chosen, repeat selection roulette selection until a new parent B is found
		while(same == true){
			//logger.warn("same "+same)
			d = r.nextDouble()
			crossoverPoint = DataModule.changeNumberRange(d, 0.0, 1.0, 0.0, valuesSum)
			cumulative = 0.0
			found = false
			(0 until mapBufKeep.values.size).foreach{
				x =>
					if(fitnessType == "linear")
					cumulative = cumulative + values.toList(x)
				else
					cumulative = cumulative + mapBufKeep.values.toList(x)
					if(cumulative >= crossoverPoint && found==false){
						found = true
						//logger.warn("found cumulative point "+ cumulative)
						pB = mapBufKeep.keys.toList(x)
					}
			}
			if(pA != pB){
				same = false
			}
		}
		//Return pair list
		parentsPair += pA
		parentsPair += pB
		parentsPair.toList
	}
	
	def operatorCrossover(numAssets:Int, parentsPair:List[DataFrame]):List[DataFrame]={
		//logger.warn("One Point Crossover Operator")
		//do crossover and get offspring pair
		val childrenPair = scala.collection.mutable.ListBuffer.empty[DataFrame]
		val pA = parentsPair(0)
		val pB = parentsPair(1)
		var r = scala.util.Random
		var p1:DataFrame = spark.emptyDataFrame
		var p2:DataFrame = spark.emptyDataFrame
		var duplicate = true
		//do loop until children don't have duplicate symbols
		while(duplicate == true){
			//Choose the partition point for One Point Crossover
			var rand = r.nextInt(numAssets+1)
			if(rand == 0)
			rand = 1
			//Get chromosomes heads and tails
			val pAHead = pA.sort(desc("symbol"))
				.limit(rand)
			val pATail = pA.sort(asc("symbol"))
				.limit(numAssets-rand)
			val pBHead = pB.sort(desc("symbol"))
				.limit(rand)
			val pBTail = pB.sort(asc("symbol"))
				.limit(numAssets-rand)
			//Switch heads and tails and re-normalize weights
			p1 = pAHead.union(pBTail)
			p2 = pBHead.union(pATail)
						
			val dup1 = p1.select("symbol")
				.distinct
				.count
			val dup2 = p2.select("symbol")
				.distinct
				.count
			if(dup1 != numAssets || dup2 != numAssets){
				duplicate = true
				}else{
					duplicate = false
				}
			}
			p1 = DataModule.normalizeWeightsDf(p1)
			p2 = DataModule.normalizeWeightsDf(p2)
			childrenPair += p1
			childrenPair += p2
			childrenPair.toList
	}
	
	def operatorMutation(portfolio:DataFrame, mutationRate:Double, sDate:String, windowInt:Int):DataFrame={
		//logger.warn("Mutate Individual by getting new weights")
		val t0 = System.nanoTime
		var r = scala.util.Random
		var p = portfolio
		var p3:DataFrame = spark.emptyDataFrame
		val m = r.nextInt(100).toDouble / 100.0 
		if(m < mutationRate){
			//decide which gene to mutate
			val g = r.nextInt(numAssets)
			val pList = p.sort(desc("weight"))
				.limit(numAssets)
				//.sort(asc("symbol"))
				.select(col("symbol"))
				.map( r => r.getString(0) )
				.collect
				.toList
			var symb = pList(g)
			var allowed = false
			var p2:DataFrame = spark.emptyDataFrame
			var aW = p.filter(col("symbol")===symb)
				.select("weight")
				.map( r => r(0).asInstanceOf[Double] )
				.collect
			var w = aW(0)
			val limit = 20
			var currentLimit = 0
			while(allowed == false){
				val t1 = System.nanoTime
				val deltaT = t1 -t0
				val mutationRuntime = TimeUnit.NANOSECONDS.toSeconds(deltaT).toDouble
				if(mutationRuntime >= 15){
					p2 = createRandomPortfolio(sDate, windowInt)
					allowed = isPortfolioWeightValid(p2)
					logger.warn("mutation "+mutationRuntime + "  " + allowed)
				}
				currentLimit = currentLimit+1
				if(currentLimit == limit){
					logger.warn("currentLimit")
					val g = r.nextInt(numAssets)
					symb = pList(g)
					//logger.warn(symb)
					aW = p.filter(col("symbol")===symb).select("weight")
						.map( r => r(0).asInstanceOf[Double] ).collect
					w = aW(0)
					currentLimit = 0
				}
				//mutate that gene - generate new weight
				var xy = r.nextInt((ceiling*1000).toInt).toDouble
				if(xy == 0.0)
					xy = 1.0
				val newW:Double = xy/1000.0
				//update on portfolio
				val p1 = p.withColumn("weight", when(col("weight").equalTo(w), newW).otherwise(col("weight")))
				//normalize portfolio
				p2 = DataModule.normalizeWeightsDf(p1)
				allowed = isPortfolioWeightValid(p2)
			}
			//val p2 = p1
			p3 = p2.sort(desc("weight"))
			//logger.warn(p3.show)
		}else{
			p3 = p
		}
		p3
	}
	
	// ALGORITHM
	
	def geneticAlgorithm(generations:Int, popSize:Int, mutationRate:Double, windowInt:Int, dDate:Date, cardinality:Int, fitnessType:String):DataFrame={
		logger.warn("geneticAlgorithm")
		
		//statistics
		val fitnessHistory = scala.collection.mutable.ListBuffer.empty[Double]
		val iterationRuntimeHistory = scala.collection.mutable.ListBuffer.empty[Double]
		var genRuntime:Double = 0.0
		val gt0 = System.nanoTime()
		
		numAssets = cardinality
		val listBuf = scala.collection.mutable.ListBuffer.empty[DataFrame]
		var mapBuf = scala.collection.mutable.LinkedHashMap.empty[DataFrame, Double]
		var newMapBuf = scala.collection.mutable.LinkedHashMap.empty[DataFrame, Double]
		
		//Population Model: Generational (pop changes every iteration)
		
		(0 until generations).foreach{
			g =>
				//count time for each generation
				val t0 = System.nanoTime
				if(g == 0){
					//Random Initialization of 1st generation
					mapBuf = initRandomPop(dDate, windowInt, popSize)
					//logger.warn("1st gen randomly init")
					//End 1st generation random init
				}
				logger.warn("-----GENERATION "+g+"-----")
				
				//Checkpoint periodically to cut RDD lineage and avoid OutOfMemory exceptions
				if((g+1)%2 == 0){
					mapBuf = checkpointPop(mapBuf)
				}
				//End of periodic checkpointing

				//Evaluate individuals in population using fitness function
				evaluatePop(fitnessType, windowInt, dDate, mapBuf)
				//End evaluation
				
				//Parent set selection
				val threshold = 0.5
				//index of the threshold selection point
				val selection:Int = popSize - (popSize*threshold).toInt
				val parents = selectionParents(popSize, mapBuf, threshold)
				var mapBufKeep = parents
				//End parent set selection
				
				//Use elitism for next Gen: keep best portfolio for next generation
				val maxFit = mapBuf.maxBy{ item => item._2 }
				//logger.warn("Best fitness in generation "+maxFit)
				fitnessHistory += maxFit._2
				val elitistP = maxFit._1
				//logger.warn("elitist "+maxFit._2)
				//logger.warn(maxFit._1.show)
				newMapBuf += (elitistP -> 0.0)
				//logger.warn("Elitism done")
				//End elitism
				
				//Need to generate popSize-1 new individuals
				
				//Do crossover selection with parents map
				//Roulette Wheel Selection for crossover
				var r = scala.util.Random
				//z: number of crossover iterations -> each iteration generates 2 offspring
				val z = ((selection-1)/2).toInt
				logger.warn("number of crossovers "+z+" : 1 crossover = 2 new individuals")
				for(i <- 0 to z){
					logger.warn(i)
					//do crossover selection and get parent pair
					val parentsPair = selectionCrossover(popSize, parents, threshold, fitnessType)
					//do crossover and get offspring pair
					val childrenPair = operatorCrossover(numAssets, parentsPair)
					listBuf += childrenPair(0)
					listBuf += childrenPair(1)
				}
				//logger.warn("roulette selection and crossovers end")
				
				//AFTER CROSSOVER, mutate the crossover children by using
				//weight variations with probability mutationRate
				//logger.warn("mutation start")
				//for each child chromosome, get final new generation mutated individual
				listBuf.par.foreach{
					p =>
						val mutatedP = operatorMutation(p, mutationRate, DataModule.dateToString(dDate), windowInt)
						newMapBuf = this.synchronized{
							newMapBuf += (mutatedP -> 0.0)
							newMapBuf
						}
				}
				listBuf.clear
				//logger.warn("mutation end")
				
				//Fill the remaining new population with random portfolios
				val diff = popSize - newMapBuf.size
				//logger.warn("Dif: "+diff)
				(0 until diff).par.foreach{
					x =>
						//logger.warn("Pop iteration "+ x)
						val p = createRandomPortfolio(dDate.toString, windowInt)
						newMapBuf = this.synchronized{
							newMapBuf += (p -> 0.0)
							newMapBuf
						}
						
				}
				mapBuf = newMapBuf
				logger.warn("newMapBuf size: "+newMapBuf.size)
				newMapBuf = scala.collection.mutable.LinkedHashMap.empty[DataFrame, Double]
				logger.warn("-----GENERATION END-----")
				
				//statistics
				val t1 = System.nanoTime
				val deltaT = t1 -t0
				val runtimeSec = TimeUnit.NANOSECONDS.toSeconds(deltaT).toDouble
				iterationRuntimeHistory += runtimeSec
		}
		
		logger.warn("Evaluate last generation")
		mapBuf = evaluatePop("return", windowInt, dDate, mapBuf)
		//sort population low to high by value
		mapBuf = scala.collection.mutable.LinkedHashMap(mapBuf.toSeq.sortWith(_._2 > _._2):_*)
		
		//Print and return optimized portfolio
		val maxFit = mapBuf.maxBy{ item => item._2 }
		//logger.warn(maxFit._1.show)
		//logger.warn(PortfolioModule.getPortfolioReturn(maxFit._1, windowInt))
		
		//statistics
		val gt1 = System.nanoTime
		val deltaT = gt1 -gt0
		genRuntime = TimeUnit.NANOSECONDS.toSeconds(deltaT).toDouble
		
		//Write optimized portfolio to file
		logger.warn("Print the best portfolio to a new output file")
		var y = 0
		var z = 0
		var sName = "portfolio"+y
		var sPath = "file:///home/osiris_wrath/Code/hdfs/runs/"+sName+".csv"
		var boolean = true
		while(boolean == true){
			sName="portfolio"+y
			sPath = "file:///home/osiris_wrath/Code/hdfs/runs/"+sName+".csv"
			try{
				maxFit._1.write.format("csv").option("header", "true").save(sPath)
				z = y
				logger.warn(z)
				boolean = false
			}catch{
				case _: Throwable => 
					boolean = true
					y = y+1
			}
		}
		//Write Statistics to File
		var listOfLists = scala.collection.mutable.ListBuffer.empty[List[Double]]
		listOfLists += fitnessHistory.toList
		listOfLists += iterationRuntimeHistory.toList
		listOfLists += List.fill(generations)(genRuntime)
		listOfLists.toList.foreach(println)
		
		sName="fitnessHistory"+y
		sPath = "file:///home/osiris_wrath/Code/hdfs/runs/"+sName+".csv"
		listOfLists(0).toDF.write.format("csv").save(sPath)
		sName="iterationRuntimeHistory"+y
		sPath = "file:///home/osiris_wrath/Code/hdfs/runs/"+sName+".csv"
		listOfLists(1).toDF.write.format("csv").save(sPath)
		sName="gaRuntime"+y
		sPath = "file:///home/osiris_wrath/Code/hdfs/runs/"+sName+".csv"
		listOfLists(2).toDF.write.format("csv").save(sPath)
		
		var tuples = fitnessHistory.zip(iterationRuntimeHistory)
			//.zip(List.fill(numAssets)(genRuntime))	
		
		
		return maxFit._1
		
	}

	
	// Chromossome: stockDF.schema + weight
	
	/*
	 * Creates a random portfolio dataframe - representing a chromossome - and normalizes its weights
	 */
	def createRandomPortfolio(sDate: String, windowInt:Int): DataFrame={
		import spark.implicits._
		val string = s"mean_${windowInt}"
		val date = DataModule.stringToDate(sDate)
		val l = scala.collection.mutable.ListBuffer.fill[Double](numAssets)(0.0)
		var r = scala.util.Random
		var b = false
		var c = true
		var sampleDf = stocksDf.filter(col("date") === date)
			.filter(col(string).isNotNull)
		//while floor and ceiling constraints are being violated, apply death penalty and re-create a random chromosome
		while(b == false){
			c = true
			(0 until numAssets).par.foreach{
				i =>
					val xy = r.nextInt(1000).toDouble
					if(xy == 0.0)
						1.0
					var k:Double = xy/1000.0
					l(i) = k
			}
			val sum = l.sum
			(0 until numAssets).par.foreach{
				i =>
					l(i) = l(i) / sum
					if(l(i) < floor || l(i) > ceiling)
						c = false
			}
			if(l.sum != 1.0)
				c = false
			b = c
		}
		sampleDf = sampleDf.drop("weight")
			.orderBy(rand)
			.limit(numAssets)
		val lSymbols = sampleDf.select("symbol")
			.map( r => r.getString(0) )
			.collect
			.toList
		val lTuples = l.zip(lSymbols)
		val lDf = lTuples.toDF("weight","symbol1") 
		sampleDf = sampleDf.join(lDf, sampleDf.col("symbol").equalTo(lDf.col("symbol1")) )
			.drop("symbol1")
		sampleDf
	}
	
	//CONSTRAINT FUNCTIONS
	/*
	 * Function that checks if the Portfolio weights are valid,
	 * according to the problem contraints
	 * Contraints:
	 * min weight: .010
	 * max weight: .250
	 */
	def isPortfolioWeightValid(portfolioDf: DataFrame): Boolean={
		//logger.warn("isPortfolioWeightValid")
		val maxWeight = 0.25
		val minWeight = 0.01
		
		var list = portfolioDf.sort(desc("weight"))
			.limit(numAssets)
			.select(col("weight"))
			.map( r => r.getDouble(0) )
			.collect
			.toList
			
		var b = true
		for(w <- list){
			if(w > maxWeight || w < minWeight){
				b = false
			}
		}
		b
	}
	
}