package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    val pointX = pointString.split(",")(0).trim().toDouble
    val pointY = pointString.split(",")(1).trim().toDouble

    val rectangleMaxX = math.max(queryRectangle.split(",")(0).trim().toDouble, queryRectangle.split(",")(2).trim().toDouble)
    val rectangleMaxY = math.max(queryRectangle.split(",")(1).trim().toDouble, queryRectangle.split(",")(3).trim().toDouble)
    val rectangleMinX = math.min(queryRectangle.split(",")(0).trim().toDouble, queryRectangle.split(",")(2).trim().toDouble)
    val rectangleMinY = math.min(queryRectangle.split(",")(1).trim().toDouble, queryRectangle.split(",")(3).trim().toDouble)

    if (pointX >= rectangleMinX && pointY >= rectangleMinY && pointX <= rectangleMaxX && pointY <= rectangleMaxY) {
      return true
    } else {
      return false
    }
    // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
