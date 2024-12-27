import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import py4j.GatewayServer
import scala.io.Source
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import sttp.client3._
import org.apache.spark.sql.Dataset
import sttp.model._
import scala.collection.mutable
import org.json.JSONObject
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.pipeline.{ Annotation, StanfordCoreNLP }
import org.apache.spark.sql.SparkSession
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.FileInputStream
import scala.collection.mutable
import org.apache.poi.ss.usermodel._
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.File
import scala.collection.mutable.Set
import org.apache.poi.ss.usermodel._
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import java.io.File
import scala.collection.mutable
object StreamingDataProcessing {

  def main(args: Array[String]): Unit = {

    //val goodWords = mutable.Set("Roasted", "egg", "awesome", "great", "fantastic", "duck", "good", "amazing", "programming", "scala")
   // val badWords = mutable.Set("sad", "hate", "frisée", "terrible", "horrible", "negative", "bad", "frustrating")


    val filePath = "C:\\Users\\Admin\\Desktop\\Sentiment-Analysis\\Copy of Positive and Negative Word List.xlsx.xlsx"

    val workbook = new XSSFWorkbook(new File(filePath))

    val sheet = workbook.getSheetAt(0)
    val badWordsSet = mutable.Set[String]()
    val goodWordsSet = mutable.Set[String]()


    for (row <- 1 to sheet.getPhysicalNumberOfRows - 1) {
      val badWordCell = sheet.getRow(row).getCell(0)
      val goodWordCell = sheet.getRow(row).getCell(1)

      if (badWordCell != null) {
        badWordsSet.add(badWordCell.getStringCellValue)
      }
      if (goodWordCell != null) {
        goodWordsSet.add(goodWordCell.getStringCellValue)
      }
    }




    val brokers = "localhost:9092"
    val groupId = "GRP1"
    val topics = "my-topic"

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("streaming_data_processing")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")

    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParams)
    )


    val line = messages.map(_.value)


    val extractedText = line.map(jsonString => {
      try {
        val json = new JSONObject(jsonString)
        val text = json.optString("text", "")
        val words = text.toLowerCase.split("\\W+")
        val goodScore = words.count(word => goodWordsSet.contains(word))
        val badScore = words.count(word => badWordsSet.contains(word))

        val sentiment = if (goodScore > badScore) "Positive"
        else if (badScore > goodScore) "Negative"
        else "Neutral"


        json.put("sentiment", sentiment)


        json.toString
      } catch {
        case e: Exception =>

          new JSONObject().put("error", e.getMessage).toString
      }

    })

    extractedText.print()



    ssc.start()
    ssc.awaitTermination()
  }
}

