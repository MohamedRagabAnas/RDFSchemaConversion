name := "RDFSchemaConversion"
version := "0.1"
scalaVersion := "2.11.12"

Compile/mainClass := Some("SingleStatementTable")

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"