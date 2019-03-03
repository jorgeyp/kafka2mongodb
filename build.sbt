name := "kafka2mongodb"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.kafka" %% "kafka" % "2.1.1"

libraryDependencies += "com.typesafe.akka" %% "akka-stream" % "2.5.21"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.21"

libraryDependencies += "com.typesafe.akka" %% "akka-http-core" % "10.1.7"

libraryDependencies += "com.typesafe.akka" %% "akka-stream-kafka" % "1.0-RC2"

libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-mongodb" % "1.0-M2"

libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.3.0"

libraryDependencies += "org.wvlet.airframe" %% "airframe-log" % "19.2.0"

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.25"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"
