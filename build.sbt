ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "ecommerce-behavior-analysis",

    Compile / resourceDirectory := baseDirectory.value / "src" / "main" / "resources",
    dependencyOverrides ++= Seq(
      "com.github.luben" % "zstd-jni" % "1.5.5-11",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-core" % "2.15.2",
      "com.fasterxml.jackson.core" % "jackson-annotations" % "2.15.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2"
    ),

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",
      "org.apache.spark" %% "spark-streaming" % "3.5.1",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.1",
      "org.apache.spark" %% "spark-mllib" % "3.5.1",
      "io.delta" %% "delta-spark" % "3.2.0",

      "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
      "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.793",

      "com.typesafe" % "config" % "1.4.3",

      "com.clickhouse" % "clickhouse-jdbc" % "0.6.4",
      "com.clickhouse" % "clickhouse-http-client" % "0.6.4",
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.2.1",
      "org.apache.httpcomponents.client5" % "httpclient5" % "5.2.1",

      "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.22"
        exclude("com.google.guava", "guava")
        exclude("com.fasterxml.jackson.core", "jackson-databind")
        exclude("com.fasterxml.jackson.core", "jackson-core")
        exclude("com.fasterxml.jackson.core", "jackson-annotations"),
      "com.google.cloud" % "google-cloud-storage" % "2.36.1"
        exclude("com.fasterxml.jackson.core", "jackson-databind")
        exclude("com.fasterxml.jackson.core", "jackson-core")
        exclude("com.fasterxml.jackson.core", "jackson-annotations"),
      "com.google.cloud.spark" %% "spark-bigquery" % "0.38.0"
        exclude("com.fasterxml.jackson.core", "jackson-databind")
        exclude("com.fasterxml.jackson.core", "jackson-core")
        exclude("com.fasterxml.jackson.core", "jackson-annotations")
    ),

    assembly / mainClass := Some("com.example.main.streaming.STREAMING"),
    assembly / assemblyJarName := "ecommerce-analysis-assembly.jar",
    assembly / test := {},
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case PathList("META-INF", "versions", _ @ _*) => MergeStrategy.first
      case PathList("META-INF", "services", _ @ _*) => MergeStrategy.filterDistinctLines
      case "reference.conf" | "application.conf" => MergeStrategy.concat
      case PathList("META-INF", "org", "apache", "logging", "log4j", "core", "config", "plugins", "Log4j2Plugins.dat") =>
        MergeStrategy.concat
      case x => MergeStrategy.first
    }
  )