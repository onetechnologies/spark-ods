package jp.co.toyo.spark.ods

import java.util._
import java.sql.Timestamp

import org.asam.ods._
import jp.co.toyo.spark.ods.ODSImplicits._

object QueryStructureExtUtil {

  def toTS_Union(dt: org.asam.ods.DataType, value: Any): TS_Union = {
    val t = dt.value()
    val u = new TS_Union()
    if (t == DataType._DT_STRING) {
      u.stringVal(value.asInstanceOf[String])
    } else if (t == DataType._DT_SHORT) {
      if (value.isInstanceOf[String]) {
        u.shortVal(value.asInstanceOf[String].toShort)
      } else {
        u.shortVal(value.asInstanceOf[Short])
      }
    } else if (t == DataType._DT_FLOAT) {
      if (value.isInstanceOf[String]) {
        u.floatVal(value.asInstanceOf[String].toFloat)
      } else {
        u.floatVal(value.asInstanceOf[Float])
      }
    } else if (t == DataType._DT_LONG) {
      if (value.isInstanceOf[String]) {
        u.longVal(value.asInstanceOf[String].toInt)
      } else {
        u.longVal(value.asInstanceOf[Integer])
      }
    } else if (t == DataType._DT_DOUBLE) {
      if (value.isInstanceOf[String]) {
        u.doubleVal(value.asInstanceOf[String].toDouble)
      } else {
        u.doubleVal(value.asInstanceOf[Double])
      }
    } else if (t == DataType._DT_LONGLONG) {
      if (value.isInstanceOf[String]) {
        u.longlongVal(value.asInstanceOf[String].toLong.toT_LONGLONG)
      } else if (value.isInstanceOf[T_LONGLONG]) {
        u.longlongVal(value.asInstanceOf[T_LONGLONG])
      } else {
        u.longlongVal(value.asInstanceOf[Long].toT_LONGLONG)
      }
    } else if (t == DataType._DT_DATE) {
      if (value.isInstanceOf[String]) {
        u.dateVal(value.asInstanceOf[String])
      } else {
        u.dateVal(value.asInstanceOf[Timestamp].toODSDate)
      }
    }
    return u;
  }

  def toAscending(s: String): Boolean = {
    s.toUpperCase() match {
      case "FALSE" | "DESCENDING" | "DESC" => false
      case _ => true
    }
  }

  def toAggrFunc(s: String): AggrFunc = {
    s.toUpperCase() match {
      case "NONE" => AggrFunc.NONE
      case "COUNT" => AggrFunc.COUNT
      case "DCOUNT" => AggrFunc.DCOUNT
      case "MIN" => AggrFunc.MIN
      case "MAX" => AggrFunc.MAX
      case "AVG" => AggrFunc.AVG
      case "STDDEV" => AggrFunc.STDDEV
      case "SUM" => AggrFunc.SUM
      case "DISTINCT" => AggrFunc.DISTINCT
      case "POINT" => AggrFunc.POINT
      case _ => throw new IllegalArgumentException("s=" + s);
    }
  }

  def toSelOpcode(s: String): SelOpcode = {
    s.toUpperCase() match {
      case "EQ" => SelOpcode.EQ
      case "NEQ" => SelOpcode.NEQ
      case "GT" => SelOpcode.GT
      case "GTE" => SelOpcode.GTE
      case "LT" => SelOpcode.LT
      case "LTE" => SelOpcode.LTE
      case "LIKE" => SelOpcode.LIKE
      case "NOTLIKE" => SelOpcode.NOTLIKE
      case "IS_NULL" => SelOpcode.IS_NULL
      case "IS_NOT_NULL" => SelOpcode.IS_NOT_NULL
      case "INSET" => SelOpcode.INSET
      case "NOTINSET" => SelOpcode.NOTINSET
      case "BETWEEN" => SelOpcode.BETWEEN
      case _ => throw new IllegalArgumentException("s=" + s);
    }
  }

  def toSelOperator(s: String): SelOperator = {
    s.toUpperCase() match {
      case "AND" => SelOperator.AND
      case "OR" => SelOperator.OR
      case "OPEN" => SelOperator.OPEN
      case "CLOSE" => SelOperator.CLOSE
      case _ => throw new IllegalArgumentException("s=" + s);
    }
  }

  def toAIDName(aa: ApplicationAttribute): AIDName = {
    new AIDName(aa.getApplicationElement().getId(), aa.getName())
  }

  def toSelAIDNameUnitId(aa: ApplicationAttribute, unitId: T_LONGLONG, aggregate: AggrFunc): SelAIDNameUnitId = {
    var sanui = new SelAIDNameUnitId()
    sanui.attr = toAIDName(aa)
    sanui.unitId = unitId
    sanui.aggregate = aggregate
    return sanui
  }

  def toJoinDef(ar: ApplicationRelation): JoinDef = {
    var jd = new JoinDef()
    jd.fromAID = ar.getElem1().getId()
    jd.toAID = ar.getElem2().getId()
    jd.refName = ar.getRelationName()
    jd.joiningType = JoinType.JTDEFAULT
    return jd
  }

  def toSelItem(aa: ApplicationAttribute, op: SelOpcode, value: Any): SelItem = {
    val sve = new SelValueExt()
    sve.attr = new AIDNameUnitId()
    sve.attr.attr = toAIDName(aa)
    sve.attr.unitId = new T_LONGLONG(0, 0)
    sve.oper = op;
    sve.value = new TS_Value();
    sve.value.flag = 15;
    sve.value.u = toTS_Union(aa.getDataType(), value)
    val si = new SelItem()
    si.value(sve)
    return si
  }

  def toSelItem(op: SelOperator): SelItem = {
    val si = new SelItem()
    si.operator(op)
    return si
  }

  def toSelOrder(aa: ApplicationAttribute, ascending: Boolean): SelOrder = {
    var so = new SelOrder()
    so.attr = new AIDName(aa.getApplicationElement().getId(), aa.getName())
    so.ascending = ascending
    return so
  }
}

class QueryStructureExtBuilder(cache: ODSCache) {
  import QueryStructureExtUtil._

  var anuSeq = Seq.empty[SelAIDNameUnitId]
  var condSeq = Seq.empty[SelItem]
  var joinSeq = Seq.empty[JoinDef]
  var groupBy = Seq.empty[AIDName]
  var orderBy = Seq.empty[SelOrder]

  def build(): QueryStructureExt = {
    val qse: QueryStructureExt = new QueryStructureExt()
    qse.anuSeq = anuSeq.toArray
    qse.condSeq = condSeq.toArray
    qse.joinSeq = joinSeq.toArray
    qse.groupBy = groupBy.toArray
    qse.orderBy = orderBy.toArray
    return qse
  }

  def select(s: String): Unit = {
    var aggregate = AggrFunc.NONE
    var aeName = s.split("\\.")(0).trim()
    var aaName = s.split("\\.")(1).trim()
    if (aeName.startsWith("DISTINCT ")) {
      aggregate = AggrFunc.DISTINCT
      aeName = aeName.split(" ")(1)
    } else if (aeName.contains("(") && aaName.endsWith(")")) {
      aggregate = toAggrFunc(aeName.substring(0, aeName.indexOf("(")))
      aeName = aeName.substring(aeName.indexOf("(") + 1)
      aaName = aaName.substring(0, aaName.length() - 1)
    }
    select(cache.getApplicationAttribute(aeName, aaName), new T_LONGLONG(), aggregate)
  }

  def select(aa: ApplicationAttribute, unitId: T_LONGLONG = new T_LONGLONG(), aggregate: AggrFunc = AggrFunc.NONE): Unit = {
    anuSeq = anuSeq :+ toSelAIDNameUnitId(aa, unitId, aggregate)
  }

  def join(s: String): Unit = {
    val elem1Name = s.split("=")(0).split("\\.")(0)
    val arName = s.split("=")(0).split("\\.")(1)
    val elem2Name = s.split("=")(1).trim()
    join(cache.getApplicationRelation(elem1Name, arName, elem2Name))
  }

  def join(ar: ApplicationRelation): Unit = {
    joinSeq = joinSeq :+ toJoinDef(ar)
  }

  def where(s: String): Unit = {
    if (s.contains(" ")) {
      val aeName = s.split(" ")(0).split("\\.")(0).trim()
      val aaName = s.split(" ")(0).split("\\.")(1).trim()
      val opName = s.split(" ")(1).trim()
      val value = s.split(" ")(2).trim()
      where(cache.getApplicationAttribute(aeName, aaName), toSelOpcode(opName), value)
    } else {
      var op = toSelOperator(s)
      condSeq = condSeq :+ toSelItem(op)
    }
  }

  def where(aa: ApplicationAttribute, op: SelOpcode, value: Any) = {
    condSeq = condSeq :+ toSelItem(aa, op, value)
  }

  def and() = {
    condSeq = condSeq :+ toSelItem(SelOperator.AND)
  }

  def or() = {
    condSeq = condSeq :+ toSelItem(SelOperator.OR)
  }

  def open() = {
    condSeq = condSeq :+ toSelItem(SelOperator.OPEN)
  }

  def close() = {
    condSeq = condSeq :+ toSelItem(SelOperator.CLOSE)
  }

  def group(s: String): Unit = {
    val aeName = s.split(" ")(0).split("\\.")(0).trim()
    val aaName = s.split(" ")(0).split("\\.")(1).trim()
    group(cache.getApplicationAttribute(aeName, aaName))
  }

  def group(aa: ApplicationAttribute): Unit = {
    groupBy = groupBy :+ toAIDName(aa)
  }

  def order(s: String): Unit = {
    val aeName = s.split(" ")(0).split("\\.")(0).trim()
    val aaName = s.split(" ")(0).split("\\.")(1).trim()
    val ascending = if (s.contains(" ")) toAscending(s.split(" ")(1).trim()) else true
    order(cache.getApplicationAttribute(aeName, aaName), ascending)
  }

  def order(aa: ApplicationAttribute, ascending: Boolean = true): Unit = {
    orderBy = orderBy :+ toSelOrder(aa, ascending)
  }
}