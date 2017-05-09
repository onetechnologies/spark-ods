package jp.co.toyo.spark.ods

import scala.collection.mutable.Map

import org.asam.ods._
import org.omg.CORBA.ORB
import org.omg.CosNaming._
import jp.co.toyo.spark.ods.ODSImplicits._

object ODSCache {
  val orb = ORB.init(new Array[String](0), System.getProperties())

  def apply(ns: String, name: String, auth: String): ODSCache = {
    val objRef = orb.string_to_object(ns)
    val ncRef = NamingContextExtHelper.narrow(objRef)

    val objFac = ncRef.resolve_str(name)
    val factory = AoFactoryHelper.narrow(objFac)

    new ODSCache(factory.newSession(auth))
  }
}

class ODSCache(session: AoSession) {

  val as = session.getApplicationStructure()
  val aea = session.getApplElemAccess()
  val aeMap = Map.empty[String, ApplicationElement]
  val aaMap = Map.empty[String, ApplicationAttribute]
  val arMap = Map.empty[String, ApplicationRelation]
  val aeNameMap = Map.empty[Long, String]

  def getSession(): AoSession = {
    return session
  }

  def getApplElemAccess(): ApplElemAccess = {
    return aea
  }

  def getgetApplicationStructure(): ApplicationStructure = {
    return as
  }

  def getApplicationElement(aeName: String): ApplicationElement = {
    aeMap.getOrElseUpdate(
      aeName,
      as.getElementByName(aeName)
    )
  }

  def getApplicationAttribute(aeName: String, aaName: String): ApplicationAttribute = {
    aaMap.getOrElseUpdate(
      aeName + "." + aaName,
      getApplicationElement(aeName).getAttributeByName(aaName)
    )
  }

  def getApplicationRelation(elem1Name: String, arName: String, elem2Name: String): ApplicationRelation = {
    arMap.getOrElseUpdate(
      elem1Name + "." + arName + "=" + elem2Name,
      getApplicationElement(elem1Name).getAllRelations().find(r =>
        r.getRelationName().equals(arName) && r.getElem2().getName().equals(elem2Name)).get
    )
  }

  def getApplicationElementName(aid: Long): String = {
    aeNameMap.getOrElseUpdate(
      aid,
      as.getElementById(aid.toT_LONGLONG).getName()
    )
  }

  def close = {
    session.close
  }
}