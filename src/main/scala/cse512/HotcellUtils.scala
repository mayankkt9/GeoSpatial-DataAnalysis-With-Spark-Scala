package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def getNeighboursCount(max_X: Int, min_X: Int, max_Y: Int, min_Y: Int, max_Z: Int, min_Z: Int, inp_X: Int, inp_Y: Int, inp_Z: Int): Int = {
    var corners = 0;
    if (inp_X == min_X || inp_X == max_X)
      corners = corners + 1

    if (inp_Y == min_Y || inp_Y == max_Y)
      corners = corners + 1

    if (inp_Z == min_Z || inp_Z == max_Z)
      corners = corners + 1

    if (corners == 1)
      return 17
    else if (corners == 2)
      return 11
    else if (corners == 3)
      return 7
    else return 26
  }

  def getSquare(num: Int): Double = {
    return (num * num).toDouble
  }

  def getGScore(sd: Double, numberOfAdjacentCells: Int, sumOfAdjacentCells: Int, numberOfCells: Int, x: Int, y: Int, z: Int, mean: Double): Double =
  {
    val numerator = sumOfAdjacentCells.toDouble - (mean * numberOfAdjacentCells.toDouble)
    val denomPart= ((numberOfCells.toDouble * numberOfAdjacentCells.toDouble) - (numberOfAdjacentCells.toDouble * numberOfAdjacentCells.toDouble)) / (numberOfCells.toDouble - 1.0)
    val denominator = sd * Math.sqrt(denomPart)
    return numerator / denominator
  }

  // YOU NEED TO CHANGE THIS PART
}

