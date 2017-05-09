package jp.co.toyo.spark.ods

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

import org.asam.ods._
import jp.co.toyo.spark.ods.ODSImplicits._

object ResultSetUtil {

  def getCount(us: TS_UnionSeq): Integer = {
    us.discriminator().value() match {
      case DataType._DT_STRING => us.stringVal().length
      case DataType._DT_SHORT => us.shortVal().length
      case DataType._DT_FLOAT => us.floatVal().length
      case DataType._DT_LONG => us.longVal().length
      case DataType._DT_DOUBLE => us.doubleVal().length
      case DataType._DT_LONGLONG => us.longlongVal().length
      case DataType._DT_DATE => us.dateVal().length
    }
  }

  def toStructType(erses: Array[ElemResultSetExt]): StructType = {
    var st = new StructType()
    for (erse <- erses) {
      val aid = erse.aid.toLong.toString
      for (nvsui <- erse.values) {
        var name = aid + "." + nvsui.valName
        var dt = nvsui.value.u.discriminator().toSparkDataType
        st = st.add(name, dt, false)
      }
    }
    return st
  }

  def toRows(erses: Array[ElemResultSetExt]): Seq[Row] = {
    var rows: Seq[Row] = Seq()
    var count = getCount(erses(0).values(0).value.u)
    for (i <- 0 until count) {
      var data: Seq[_] = Seq()
      for (erse <- erses) {
        for (nvsui <- erse.values) {
          var us = nvsui.value.u;
          us.discriminator().value() match {
            case DataType._DT_STRING => data = data :+ us.stringVal()(i)
            case DataType._DT_SHORT => data = data :+ us.shortVal()(i)
            case DataType._DT_FLOAT => data = data :+ us.floatVal()(i)
            case DataType._DT_LONG => data = data :+ us.longVal()(i)
            case DataType._DT_DOUBLE => data = data :+ us.doubleVal()(i)
            case DataType._DT_LONGLONG => data = data :+ us.longlongVal()(i).toLong
            case DataType._DT_DATE => data = data :+ us.dateVal()(i).toTimestamp
          }
        }
      }
      rows = rows :+ Row.fromSeq(data)
    }
    return rows
  }
}

class ResultSetReader(cache: ODSCache, qse: QueryStructureExt) {
  import ResultSetUtil._

  def schema(): StructType = {
    var rses = cache.getApplElemAccess().getInstancesExt(qse, 1)
    var st = new StructType()
    for (erse <- rses(0).firstElems) {
      for (nvsui <- erse.values) {
        var name = cache.getApplicationElementName(erse.aid.toLong) + "." + nvsui.valName
        var dt = nvsui.value.u.discriminator().toSparkDataType
        st = st.add(name, dt, false)
      }
    }
    return st
  }

  def values(max: Integer): Seq[Row] = {
    var rses = cache.getApplElemAccess().getInstancesExt(qse, max)
    toRows(rses(0).firstElems)
  }
}