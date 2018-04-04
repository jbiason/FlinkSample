resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  "velvia maven" at "http://dl.bintray.com/velvia/maven",
  Resolver.mavenLocal)

name := "sideoutput-sample"

version := "0.0.1"

organization := "com.azion"

scalaVersion in ThisBuild := "2.11.8"

val flinkVersion = "1.4.2"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % "1.7.25"
)

mainClass in assembly := Some("com.azion.SideouputSample")

// make run command include the provided dependencies
run in Compile := Defaults.runTask(fullClasspath in Compile,
                                   mainClass in (Compile, run),
                                   runner in (Compile,run)
                                  ).evaluated

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

