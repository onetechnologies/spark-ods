package jp.co.toyo.spark.ods

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.MetadataBuilder

import org.asam.ods._
import jp.co.toyo.spark.ods.ODSImplicits._

object ValueMatrixUtil {

  def toStructField(column: org.asam.ods.Column): StructField = {
    val name = column.getName()
    val dataType = column.getDataType().toSparkDataType
    val mb = new MetadataBuilder()
    mb.putString("unit", column.getUnit())
    mb.putString("independent", column.isIndependent().toString())
    mb.putString("sequence_representation", column.getSequenceRepresentation().toString())
    val md = mb.build()
    StructField(name, dataType, false, md)
  }

  def getColumns(vm: ValueMatrix, names: Array[String]): Array[org.asam.ods.Column] = {
    if (names.nonEmpty) {
      names.map(name => vm.getColumns(name)(0))
    } else {
      vm.getColumns("*")
    }
  }

  def toStructType(vm: ValueMatrix, columnNames: Array[String]): StructType = {
    val columns = getColumns(vm, columnNames)
    try {
      var schema = new StructType()
      for (column <- columns) {
        val field = toStructField(column)
        schema = schema.add(field)
      }
      schema
    } finally {
      for (column <- columns) {
        column.destroy()
      }
    }
  }

  def toRow(nvui: NameValueUnitIterator): Row = {
    var data: Seq[_] = Seq()
    for (col <- 0 to nvui.getCount() - 1) {
      var nvu = nvui.nextOne()
      var u = nvu.value.u
      var value = u.discriminator().value()
      if (value == DataType._DT_STRING) {
        data = data :+ u.stringVal()
      } else if (value == DataType._DT_FLOAT) {
        data = data :+ u.floatVal()
      } else if (value == DataType._DT_LONG) {
        data = data :+ u.longVal()
      } else if (value == DataType._DT_DOUBLE) {
        data = data :+ u.doubleVal()
      } else if (value == DataType._DT_LONGLONG) {
        data = data :+ u.longlongVal().toLong
      } else if (value == DataType._DT_DATE) {
        data = data :+ u.dateVal().toTimestamp
      }
    }
    Row.fromSeq(data)
  }

  def toRows(vm: ValueMatrix, start: Int, count: Int): Seq[Row] = {
    var rows: Seq[Row] = Seq()
    for (i <- start to count - 1) {
      var nvui = vm.getValueMeaPoint(i)
      rows = rows :+ toRow(nvui)
      nvui.destroy()
    }
    rows
  }

  def toRows(vm: ValueMatrix, start: Int, count: Int, channelNames: Array[String]): Seq[Row] = {
    val columns = getColumns(vm, channelNames)
    val nvsus = try {
      vm.getValue(columns, start, count)
    } finally {
      for (column <- columns) {
        column.destroy()
      }
    }

    var rows: Seq[Row] = Seq()
    for (i <- 0 until count) {
      var data: Seq[_] = Seq()
      for (nvsu <- nvsus) {
        var us = nvsu.value.u;
        us.discriminator().value() match {
          case DataType._DT_STRING => data = data :+ us.stringVal()(i)
          case DataType._DT_FLOAT => data = data :+ us.floatVal()(i)
          case DataType._DT_LONG => data = data :+ us.longVal()(i)
          case DataType._DT_DOUBLE => data = data :+ us.doubleVal()(i)
          case DataType._DT_LONGLONG => data = data :+ us.longlongVal()(i).toLong
          case DataType._DT_DATE => data = data :+ us.dateVal()(i).toTimestamp
        }
      }
      rows = rows :+ Row.fromSeq(data)
    }
    rows
  }

  def getValueMatrix(session: AoSession, id: Long): ValueMatrix = {
    val as = session.getApplicationStructure()
    val ae = as.getElementsByBaseType("AoSubmatrix")(0)
    val ie = ae.getInstanceById(id.toT_LONGLONG)
    try {
      val sm = ie.upcastSubMatrix()
      try {
        sm.getValueMatrixInMode(ValueMatrixMode.CALCULATED)
      } finally {
        sm.destroy()
      }
    } finally {
      ie.destroy()
    }
  }
}

class ValueMatrixReader(cache: ODSCache, smid: Long, columnNames: Array[String]) {
  import ValueMatrixUtil._

  def schema(): StructType = {
    val vm = getValueMatrix(cache.getSession(), smid)
    try {
      toStructType(vm, columnNames)
    } finally {
      vm.destroy()
    }
  }

  def values(): Seq[Row] = {
    val vm = getValueMatrix(cache.getSession(), smid)
    try {
      val start = 0
      val count = vm.getRowCount()
      toRows(vm, start, count, columnNames)
    } finally {
      vm.destroy()
    }
  }
}
