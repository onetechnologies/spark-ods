package jp.co.toyo.spark.ods

import java.text.SimpleDateFormat
import java.sql.Timestamp

import org.asam.ods._

object ODSImplicits {
  implicit class T_LONGLONGExtension(tll: T_LONGLONG) {
    def toLong: Long = {
      // FIXME
      tll.high * 0x100000000L + tll.low
    }
  }

  implicit class LongExtension(l: Long) {
    // FIXME
    def toT_LONGLONG: T_LONGLONG = {
      val high = l >> 32 & 0xffffffffL
      val low = l & 0xffffffffL
      new T_LONGLONG(high.toInt, low.toInt)
    }
  }

  implicit class TimestampExtension(ts: Timestamp) {
    def toODSDate: String = {
      // TODO
      val f = new SimpleDateFormat("yyyyMMddHHmmssSSS")
      f.format(ts)
    }
  }

  implicit class StringExtension(s: String) {
    def toTimestamp: Timestamp = {
      val year = s.substring(0, 4).toInt - 1900
      val month = s.substring(4, 6).toInt - 1
      val day = s.substring(6, 8).toInt
      val hour = if (s.length >= 10) { s.substring(8, 10).toInt } else 0
      val minutes = if (s.length >= 12) { s.substring(10, 12).toInt } else 0
      val second = if (s.length >= 14) { s.substring(12, 14).toInt } else 0
      val nano = if (s.length >= 17) { s.substring(14, 16).toInt } else 0

      new Timestamp(year, month, day, hour, minutes, second, nano)
    }
  }

  implicit class DataTypeExtension(dt: DataType) {
    def toSparkDataType: org.apache.spark.sql.types.DataType = {
      dt.value() match {
        case DataType._DT_STRING => org.apache.spark.sql.types.StringType
        case DataType._DT_SHORT => org.apache.spark.sql.types.ShortType
        case DataType._DT_FLOAT => org.apache.spark.sql.types.FloatType
        case DataType._DT_LONG => org.apache.spark.sql.types.IntegerType
        case DataType._DT_DOUBLE => org.apache.spark.sql.types.DoubleType
        case DataType._DT_LONGLONG => org.apache.spark.sql.types.LongType
        case DataType._DT_DATE => org.apache.spark.sql.types.TimestampType
      }
    }
  }
}