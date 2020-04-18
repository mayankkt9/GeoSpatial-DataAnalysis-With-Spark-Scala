package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  val cellCoordinates = spark.sql("select x, y, z from pickupInfo where x <= "+ maxX +" and x >= "+ minX +
    " and y <= "+ maxY + " and y >= "+ minY +
    " and z <= "+ maxZ +" and z >= "+ minZ +"order by z, y, x desc").persist()
  cellCoordinates.createOrReplaceTempView("cellCoordinates")

  val cellHotness = spark.sql("Select x, y, z, count(*) as NumberOfCells from cellCoordinates group by x,y,z").persist()
  cellHotness.createOrReplaceTempView("cellHotness")

  val totalPoints = spark.sql("select sum(NumberOfCells) as TotalHotCells from cellHotness")
  totalPoints.createOrReplaceTempView("totalPoints")

  val mean = (totalPoints.first().getLong(0).toDouble / numCells.toDouble).toDouble

  spark.udf.register("getSquare", (inputX: Int) => HotcellUtils.getSquare(inputX))
  val sumSquaredCells = spark.sql("select sum(getSquare(NumberOfCells)) as sumOfSquaredCell from cellHotness")
  sumSquaredCells.createOrReplaceTempView("sumSquaredCells")

  val standardDeviation = Math.sqrt((sumSquaredCells.first().getDouble(0).toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble)).toDouble

  spark.udf.register("CountOfNeighbours", (maxX: Int, minX: Int, maxY: Int, minY: Int, maxZ: Int, minZ: Int, inputX: Int, inputY: Int, inputZ: Int)
  => HotcellUtils.getNeighboursCount(maxX, minX, maxY, minY, maxZ, minZ, inputX, inputY, inputZ))
  val totalNeighbours = spark.sql("select CountOfNeighbours(" + maxX + "," + minX + "," + maxY + "," + minY + "," + maxZ + "," + minZ + "," + "ch.x, ch.y, ch.z) as neighboursCells, " +
    "count(*) as totalNeighbourCells, ch.x as x, ch.y as y, ch.z as z, sum(chs.NumberOfCells) as calculatedValue "+
    "from cellHotness as ch, cellHotness as chs "+
    "where (chs.x = ch.x + 1 or chs.x = ch.x or chs.x = ch.x - 1) and (chs.y = ch.y + 1 or chs.y = ch.y or chs.y =ch.y - 1) and (chs.z = ch.z + 1 or chs.z = ch.z or chs.z = ch.z - 1) group by ch.z, ch.y, ch.x order by ch.z, ch.y, ch.x").persist()
  totalNeighbours.createOrReplaceTempView("totalNeighbours")

  spark.udf.register("CalculateGScore", (sd: Double, numberOfAdjacentCells: Int, sumOfAdjacentCells: Int, numberOfCells: Int, x: Int, y: Int, z: Int, mean: Double)
  => HotcellUtils.getGScore(sd,numberOfAdjacentCells,sumOfAdjacentCells,numberOfCells,x,y,z,mean))
  val GScore = spark.sql("select GScore("+standardDeviation+", neighboursCells, calculatedValue,"+numCells+"x,y,z,"+mean+") as GScore, x, y, z from totalNeighbours order by GScore desc")
  GScore.createOrReplaceTempView("GScore")

  val result = spark.sql("select x, y, z from GScore")
  result.createOrReplaceTempView("result")
  return result // YOU NEED TO CHANGE THIS PART
}
}
