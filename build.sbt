scalaVersion := "2.13.8"


libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.2.1",
  "org.apache.kafka" % "kafka-streams" % "3.2.1",
  "org.apache.kafka" %% "kafka-streams-scala" % "3.2.1",
  "io.circe" %% "circe-core" % "0.14.2",
  "io.circe" %% "circe-generic" % "0.14.2",
  "io.circe" %% "circe-parser" % "0.14.2"
)
