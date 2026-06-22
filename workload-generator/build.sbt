/*
 * Copyright (2025) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

organization := "io.delta"
name := "delta-workload-generator"
version := "0.1.0"
scalaVersion := "2.13.17"
crossScalaVersions := Seq("2.12.20", "2.13.17")
publishMavenStyle := true
resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

// JVM module options for Java 17+ (required for Spark 4.x)
val jvmOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "delta-workload-generator",
    libraryDependencies ++= {
      // delta-spark 4.x is Scala 2.13 only; 3.2.x is the last 2.12 release
      val (deltaV, sparkV) = if (scalaBinaryVersion.value == "2.13")
        ("4.1.0", "4.1.0") else ("3.2.1", "3.5.4")
      Seq(
        // provided: DAT test runners should already have these dependencies, so the published jar
        // should not pull in its own versions, which may clash.
        "io.delta" %% "delta-spark" % deltaV % "provided",
        "org.apache.spark" %% "spark-sql" % sparkV % "provided",
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2" % "provided",
        "commons-io" % "commons-io" % "2.11.0" % "provided",
        "org.scalatest" %% "scalatest" % "3.2.19" % "provided",
        // Test dependencies
        "io.delta" %% "delta-spark" % deltaV % "test",
        "org.apache.spark" %% "spark-sql" % sparkV % "test",
        "org.scala-lang" % "scala-compiler" % scalaVersion.value % "test"
      )
    },
    Test / fork := true,
    Test / javaOptions ++= jvmOptions,
    // Spark requires a single SparkContext per JVM — suites must run sequentially
    Test / parallelExecution := false,
    // Bundle the `io.delta.workload.tables.*` suites into the main jar. These
    // suites are workload specifications disguised as ScalaTest suites: they
    // describe tables other engines should be able to read, not assertions
    // about DAT itself. Downstream consumers (e.g. a DBR runtime packaging)
    // fetch the main jar and scalatest-discovers them via `-R`. Meta-tests
    // under `io.delta.workload.{WorkloadGeneratorSuite, WorkloadValidatorSuite}`
    // and `io.delta.workload.log.*` stay test-only.
    Compile / packageBin / mappings ++= {
      val _ = (Test / compile).value  // ensure test classes are built
      val testClassDir = (Test / classDirectory).value
      val tablesDir = testClassDir / "io" / "delta" / "workload" / "tables"
      if (tablesDir.exists) {
        Path.allSubpaths(tablesDir).map { case (file, relPath) =>
          (file, s"io/delta/workload/tables/$relPath")
        }.toSeq
      } else Seq.empty
    },
    // Include provided dependencies on the runtime classpath for runMain
    Compile / run / fork := true,
    Compile / run / javaOptions ++= jvmOptions,
    Runtime / fullClasspath ++= (Compile / managedClasspath).value.filter { jar =>
      val name = jar.data.getName
      name.contains("delta") || name.contains("spark")
    },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
