package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = { 

      try{
        val point = pointString.split(",")
        val rectangle = queryRectangle.split(",")
        val point_x = point(0).trim().toDouble
        val point_y = point(1).trim().toDouble
        val rec_x1 = rectangle(0).trim().toDouble
        val rec_y1 = rectangle(1).trim().toDouble
        val rec_x2 = rectangle(2).trim().toDouble
        val rec_y2 = rectangle(3).trim().toDouble
        val low_x = scala.math.min(rec_x1, rec_x2)
        val high_x = scala.math.max(rec_x1, rec_x2)
        val low_y = scala.math.min(rec_y1, rec_y2)
        val high_y = scala.math.max(rec_y1, rec_y2)

        if(point_y >= low_y && point_y <= high_y && point_x >= low_x && point_x <= high_x){
            return true
        }
        else{
            return false
        }
    }
    catch {
        case _: Throwable => return false
    }
  }

 

}
