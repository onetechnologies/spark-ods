package jp.co.toyo.spark.ods

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{ RelationProvider, DataSourceRegister, BaseRelation }

class DefaultSource extends RelationProvider with DataSourceRegister {
  override def shortName(): String = "ods"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    return new ODSRelation(parameters)(sqlContext)
  }
}

