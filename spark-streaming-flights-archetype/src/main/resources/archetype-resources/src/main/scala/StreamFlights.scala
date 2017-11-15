import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import com.fasterxml.jackson.databind.{ DeserializationFeature, ObjectMapper }
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Get
import org.apache.hadoop.hbase.util.Bytes

object StreamFlights {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.quorum","localhost")
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("weather_delays_by_route_new"))
  
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
        |Usage: StreamFlights <brokers>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamFlights")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set[String]("flights")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    // Get the lines, split them into words, count the words and print
    val serializedRecords = messages.map(_._2);

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaFlightRecord]))

    // How to read from an HBase table
    val batchStats = kfrs.map(kfr => {
      val result = table.get(new Get(Bytes.toBytes(kfr.originName + kfr.destinationName)))
      if(result == null || result.getRow() == null)
        RouteStats(kfr.originName, kfr.destinationName)
      else
        RouteStats(kfr.originName, kfr.destinationName,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("clear_flights"))).toInt,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("clear_delays"))).toDouble,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("fog_flights"))).toInt,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("fog_delays"))).toDouble,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("rain_flights"))).toInt,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("rain_delays"))).toDouble,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("snow_flights"))).toInt,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("snow_delays"))).toDouble,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("hail_flights"))).toInt,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("hail_delays"))).toDouble,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("thunder_flights"))).toInt,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("thunder_delays"))).toDouble,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("tornado_flights"))).toInt,
        Bytes.toString(result.getValue(Bytes.toBytes("delay"), Bytes.toBytes("tornado_delays"))).toDouble)
    })
    
    // Your homework is to get a speed layer working
    //
    // In addition to reading from HBase, you will likely want to
    // either insert into HBase or increment existing values in HBase
    // You can do these just like the above, but instead of using a
    // Get object, you use a Put or Increment objects as documented here:
    //
    // http://javadox.com/org.apache.hbase/hbase-client/1.1.2/org/apache/hadoop/hbase/client/Put.html
    // http://javadox.com/org.apache.hbase/hbase-client/1.1.2/org/apache/hadoop/hbase/client/Increment.html
    //
    // One nuisance is that you can only increment by a Long, so
    // I have rebuilt our tables with Longs instead of Doubles
    
    // For now, this just prints the batch data we looked up to the console.
    // You can drop this once you have written the appropriate HBase code
    batchStats.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}