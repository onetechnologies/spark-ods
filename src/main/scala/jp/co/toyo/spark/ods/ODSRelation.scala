package jp.co.toyo.spark.ods

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ SQLContext, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.{ RelationProvider, BaseRelation, TableScan }

import org.asam.ods._

class ODSRelation(parameters: Map[String, String])(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan with Serializable {
  val ns = parameters("ns")
  val name = parameters("name")
  val auth = parameters("auth")

  override val schema: StructType = inferSchema()

  override def buildScan(): RDD[Row] = {
    val cache = ODSCache(ns, name, auth)
    try {
      val rows = if (parameters.contains("submatrix")) {
        createValueMatrixReader(cache, parameters).values()
      } else {
        val qse = createQuery(cache, parameters)
        new ResultSetReader(cache, qse).values(0)
      }
      sqlContext.sparkContext.parallelize(rows)
    } catch {
      case e: AoException => System.err.println(e.reason); throw e
    } finally {
      cache.close
    }
  }

  private def inferSchema(): StructType = {
    val cache = ODSCache(ns, name, auth)
    try {
      return if (parameters.contains("submatrix")) {
        createValueMatrixReader(cache, parameters).schema()
      } else {
        val qse = createQuery(cache, parameters)
        new ResultSetReader(cache, qse).schema()
      }
    } catch {
      case e: AoException => System.err.println(e.reason); throw e
    } finally {
      cache.close
    }
  }

  private def createValueMatrixReader(cache: ODSCache, parameters: Map[String, String]): ValueMatrixReader = {
    val smid = parameters("submatrix").toLong
    val channels = if (parameters.contains("channels")) {
      parameters("channels").split(",").map(s => s.trim())
    } else {
      Array[String]()
    }
    new ValueMatrixReader(cache, smid, channels)
  }

  private def createQuery(cache: ODSCache, parameters: Map[String, String]): QueryStructureExt = {
    val builder = new QueryStructureExtBuilder(cache)
    parameters.foreach {
      case (key, value) =>
        key.toUpperCase() match {
          case "SELECT" => value.split(",").map(s => s.trim()).foreach(builder.select)
          case "JOIN" => value.split(",").map(s => s.trim()).foreach(builder.join)
          case "WHERE" => value.split(",").map(s => s.trim()).foreach(builder.where)
          case "GROUP BY" => value.split(",").map(s => s.trim()).foreach(builder.group)
          case "ORDER BY" => value.split(",").map(s => s.trim()).foreach(builder.order)
          case _ =>
        }
    }
    builder.build()
  }
}