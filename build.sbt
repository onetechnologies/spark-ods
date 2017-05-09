organization := "jp.co.toyo.spark.ods"
name         := "spark-ods"
version      := "0.1.1"

scalaVersion := "2.11.7"

import sbt.Package.ManifestAttributes
packageOptions := Seq(ManifestAttributes(
		("Implementation-Vendor", "TOYO Corporation"),
		("Implementation-Vendor-Id", "jp.co.toyo"),
		("Specification-Vendor", "TOYO Corporation")))

val scalatestV = "3.0.0"
libraryDependencies += "org.scalactic" %% "scalactic" % scalatestV
libraryDependencies += "org.scalatest" %% "scalatest" % scalatestV % "test"
libraryDependencies += "org.asam" % "ods" % "5.3.0"

spName := "toyo/spark-ods"
sparkVersion := "2.0.0"
sparkComponents += "sql"
spAppendScalaVersion := false
spIncludeMaven := false
