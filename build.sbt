
name := "Spark certification"
version := "1.0"

artifactName := {
  (sv: ScalaVersion, module: ModuleID, artifact: Artifact) => "spark-certification.jar"
}

scalaVersion := "2.10.5"
scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-optimise", "-target:jvm-1.7")

libraryDependencies ++= Seq(

	"org.apache.spark" %% "spark-core" % "1.5.1" withSources() withJavadoc(),
	"org.apache.spark" %% "spark-sql" % "1.5.1" withSources() withJavadoc(),
	"org.apache.spark" %% "spark-hive" % "1.5.1" withSources() withJavadoc(),
	"org.apache.spark" %% "spark-catalyst" % "1.5.1" withSources() withJavadoc(),

	"org.scalatest"    %% "scalatest" % "2.2.4" % "test" withSources() withJavadoc()
)

scalastyleConfig := file("project/scalastyle-config.xml")

parallelExecution in Test := false