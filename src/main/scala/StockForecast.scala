// scalastyle:off println

package weekEndProjects.streamingApps

import kafka.serializer.StringDecoder
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import com.cloudera.sparkts.models.ARIMA
import org.apache.spark.mllib.linalg.Vectors
import com.datastax.spark.connector.streaming._

// structure for aggregated data
case class streamingTuple (
  name : String,
  sum : Double,
  count : Double,
  avg : Double,
  timestamp : String,
  uniquekey : Int
)

object StockForecast {

/* trains ARIMA model
   input : historic data
   output : trained model */
  def trainModel(trainData : Array[Double]) : com.cloudera.sparkts.models.ARIMAModel = {
    
    // convert from array to vectosr type
    val vectors = Vectors.dense(trainData)
    val arimaModel = ARIMA.autoFit(vectors)
    arimaModel
  }

  def main(args: Array[String]) {

    // kafka broker
    val brokers = "localhost:9092"
    val topics = "test"

    // Create context with 3 second batch interval, processing time : ~100ms, scheduling delay : 0
    val sparkConf = new SparkConf().setAppName("ForecastingSandBox")
                        .set("spark.cassandra.connection.host", "127.0.0.1")
                        .setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // read historic data
    val data = new SQLContext(ssc.sparkContext).read.format("com.databricks.spark.csv").option("header", "true")
      .option("inferSchema","true").load("/home/vdep/stockData/acnTrain.csv")

    // initial forcasting
    var avgValue = data.select("V6").rdd.map(x => x(0).toString.toDouble).collect
    var arimaModel = trainModel(avgValue)
    var forecast = arimaModel.forecast(Vectors.dense(avgValue),100) 

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)

   // val count = ssc.sparkContext.accumulator(0)
    var count : Int = 0
    // total datapoints in the historic data
    var existingDataPoints = 2534
    var previousValue : Double = 52.2307
    val grouped = lines.flatMap(_.split(" ")).map{ x =>
      val quotes = x.split(":")
      (quotes(0), quotes(1).toDouble)
    }


    grouped.foreachRDD { (rdd: RDD[(String,Double)] ,time: Time) =>

      // computing sum and count of the respective keys with timeStamp
      val combined = rdd.combineByKey( (x: Double) => (x,1.0, time.toString),
         (aggregatedPairs: (Double, Double, String), x: Double) => 
            (aggregatedPairs._1 +x, aggregatedPairs._2 +1.0, aggregatedPairs._3),
         (aggregatedPairs1: (Double, Double, String), aggregatedPairs2: (Double, Double, String)) => 
            (aggregatedPairs1._1 + aggregatedPairs2._1, aggregatedPairs1._2 + aggregatedPairs2._2, aggregatedPairs1._3)
     )


      // type alias
      type aggregatedTableType = (String, (Double, Double, String))
      def apply(acc: aggregatedTableType)  = {
        val (key,(sum, count, timeInstance)) = acc
        (key, sum, count, sum/count, timeInstance.split(" ")(0),scala.util.Random.nextInt(2000000))
      }

      /* val finalResult = combined.map { record =>
         streamingTuple(record._1, record._2._1, record._2._2, (record._2._1/record._2._2), record._2._3.split(" ")(0),scala.util.Random.nextInt(2000000))
     }  */
     
     /* since the batch intreval is different for different data source, the data is aggregated 
        so that the batch intreval is common for all data sources */
      val finalResult = combined.map { apply }
      /* saves the data in the RDD at the current interval to cassandra table
         test.averaged is the cassandra table */
      finalResult.saveToCassandra("test","averaged", SomeColumns("name", "sum", "count", "avg", "timestamp", "uniquekey"))

      // filter the particular key(company), saving preedicted values to cassandra table 
  //    val accumulatorToInt : Int = count.value
      finalResult.filter(x => x._1 == "ACN").map { row =>

        val predictedValue = forecast( existingDataPoints + count)

        val predictionTableRow = (predictedValue, if(previousValue - predictedValue < 0 ) "increase" else "decrease", 
             (forecast( existingDataPoints + count -1)/previousValue -1) * 100,  scala.util.Random.nextInt(2000000))
 
        previousValue = row._4
        predictionTableRow
       }.saveToCassandra("test", "prediction", SomeColumns("forecastvalue","trend","percentagechange", "uniquekey"))  
        

       count = count + 1
       // adding current row to historic data
       val currentRow = finalResult.filter(x => x._1 == "ACN").map( x => x._2).collect
       avgValue = avgValue ++ currentRow
       // The model is update once for every 100 batch intreval to incorporate latest data to the existing model
         if(count > 100) {
            // resetting accumlulator to 0
            count = 0
            // train the model again with historic data along with new data
            arimaModel = trainModel(avgValue)
            forecast = arimaModel.forecast(Vectors.dense(avgValue),100)
            // update the total data points
            existingDataPoints = existingDataPoints + 100
          }      
      //  finalResult.coalesce(1).saveAsTextFile("file:///home/vdep/foreachrdd/temp"+time)
    }

    // Start the computation  

   // ssc.checkpoint("/home/vdep/kafkaOutput/kafkachkpt/")
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println

//spark-submit --jars /home/vdep/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/spark-cassandra-connector-assembly-1.6.0-M2-1-g48849f5.jar /home/vdep/Documents/spark_cassandra/target/scala-2.10/spark_cassandra_2.10-1.0.jar

//spark-submit --jars /home/vdep/Downloads/spark-streaming-kafka-assembly_2.10-1.3.0.jar,/home/vdep/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/spark-cassandra-connector-assembly-1.6.0-M2-1-g48849f5.jar /home/vdep/Documents/kafkaDirect/target/scala-2.10/kafkadirect_2.10-1.0.jar localhost:9092 test

