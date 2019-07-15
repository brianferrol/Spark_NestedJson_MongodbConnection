name := "intro_spark_soluciones"
version := "0.1"
scalaVersion := "2.11.12"

//Ejercicio1
//1.A Configurar el fichero build.sbt para incluir las librerias de Spark
val sparkVersion = "2.4.0"

// My dependencies
libraryDependencies ++= Seq(
 "com.google.code.gson" % "gson" % "2.8.5" % "compile"
)

// Spark dependencies
lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core"      % sparkVersion,
  "org.apache.spark" %% "spark-sql"       % sparkVersion,
  "org.apache.spark" %% "spark-mllib"     % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive"      % sparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector"  % sparkVersion
)

libraryDependencies ++= sparkDependencies.map(_ % "provided")
run in Compile := { Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)) }

// Trick to allow run the project under IDEA
lazy val mainRunner = project.in(file("mainRunner"))
  .dependsOn(RootProject(file(".")))
  .settings(libraryDependencies ++= sparkDependencies.map(_ % "compile"))
  .settings(scalaVersion := "2.11.12")
  .disablePlugins(sbtassembly.AssemblyPlugin)
