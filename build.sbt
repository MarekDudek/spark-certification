
name := "Training project for Spark certification"
version := "1.0"

scalaVersion := "2.11.7"
scalacOptions ++= Seq("-deprecation", "-explaintypes", "-feature", "-unchecked", "-optimise", "-target:jvm-1.8")

libraryDependencies ++= Seq(

	"org.apache.spark" %% "spark-core" % "1.5.1" withSources() withJavadoc(),

	"org.scalatest"    %% "scalatest" % "2.2.4" % "test" withSources() withJavadoc()
)

