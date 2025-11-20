name := "spark"

// libraryDependencies ++= Seq(
//   "org.apache.spark" %% "spark-core" % "3.5.1" % Provided,
//   "org.apache.spark" %% "spark-sql"  % "3.5.1" % Provided
// )
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql"  % "3.5.1"
)

// Main class for your Spark job
Compile / mainClass := Some("org.cscie88c.spark.SparkJob")

// Assembly plugin settings
assembly / mainClass := Some("org.cscie88c.spark.SparkJob")
assembly / assemblyJarName := "SparkJob.jar"
assembly / test := {}
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("shapeless.**" -> "shadeshapeless.@1").inAll
)

// ---------------------------
// Fix Java 17 module access for Spark tests

// Fork a separate JVM for tests
Test / fork := true

// Add JVM options to export internal Java modules for Spark
Test / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
// ---------------------------
