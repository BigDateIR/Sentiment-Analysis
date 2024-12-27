
ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "tweets processing",
      libraryDependencies ++= Seq(
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
   "org.apache.spark" %% "spark-core" % "3.0.3",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
   "org.apache.spark" %% "spark-streaming" % "3.0.3" ,
    // https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
     "org.apache.kafka" % "kafka-clients" % "3.8.1",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
     "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.0.3",
   "org.json" % "json" % "20230618",



   "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.0.0",

   "org.tensorflow" % "tensorflow-core-platform" % "0.4.0",

     "org.apache.spark" %% "spark-sql" % "3.3.1",
     "org.apache.spark" %% "spark-mllib" % "3.5.0",

   "com.johnsnowlabs.nlp" %% "spark-nlp-silicon" % "5.5.0",
    "edu.stanford.nlp" % "stanford-corenlp" % "4.5.1",

    "org.jfree" % "jfreechart" % "1.5.3",
        "com.softwaremill.sttp.client3" %% "core" % "3.8.3",
          "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.0.0" % "provided" ,
        "org.apache.poi" % "poi-ooxml" % "5.2.3"),

        dependencyOverrides += "com.github.luben" % "zstd-jni" % "1.5.6-4"

  )