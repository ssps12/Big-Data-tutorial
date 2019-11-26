// Implement batch layer for Flight and Weather app in Spark Scala
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object SparkBatchLayer {
 def main(args: Array[String]) {
    // You can skip past the creation of spark if you
    // are in the spark-shell because it is created for you
    // but if you use spark-submit, you will need to create
    // a SparkSession yourself
    val conf = new SparkConf().setAppName("Build SD Table").setMaster("local[2]");
    // Create a spark session
    val spark = SparkSession
			.builder()
			.appName("Build SD Table")
			.config("spark.hadoop.metastore.catalog.default", "hive")
			.enableHiveSupport()
			.getOrCreate()
    
    spark.sql("add jar hdfs:///tmp/IngestWeather-0.0.1-SNAPSHOT.jar")
    val ontime = spark.table("ontime")
    val ws = spark.table("weathersummary")
    val stations = spark.table("stations")

    // SparkSQL can be used in two ways. The first way is to just shove Hive queries into a sql() statement,
    // while the second is to explicitly use dataframe operations. We demonstrate both for constructing
    // the flights_and_weather table.
    
    // Hive style. The following SQL is copied directly from join_stations.hql and join_flights_to_weather.hql
    // (Except I changed  the delays names to not conflict with our hive implementation)
    def fawHiveStyle() = {
      // Triple quotes contain multiline strings in Scala
      val dlys = spark.sql("""select t.year as year, t.month as month, t.dayofmonth as day,
        t.carrier as carrier, t.flightnum as flight,
        t.origin as origin_name, t.origincityname as origin_city, so.code as origin_code, t.depdelay as dep_delay,
        t.dest as dest_name, t.destcityname as dest_city, sd.code as dest_code, t.arrdelay as arr_delay
        from ontime t join stations so join stations sd
          on t.origin = so.name and t.dest = sd.name""") 
  
      // To make Spark dataframes visible in SQL queries, use createOrReplaceTempView
      dlys.createOrReplaceTempView("dlys")
      spark.sql("""select d.year as year, d.month as month, d.day as day, d.carrier as carrier, d.flight as flight,
        d.origin_name as origin_name, d.origin_city as origin_city, d.origin_code as origin_code, d.dep_delay as dep_delay,
        d.dest_name as dest_name, d.dest_city as dest_city, d.dest_code as dest_code, d.arr_delay as arr_delay,
        w.meantemperature as mean_temperature, w.meanvisibility as mean_visibility, w.meanwindspeed as mean_windspeed,
        w.fog as fog, w.rain as rain, w.snow as snow, w.hail as hail, w.thunder as thunder, w.tornado as tornado,
        if(w.fog,d.dep_delay,0) as fog_delay, if(w.rain,d.dep_delay,0) as rain_delay, if(w.snow, d.dep_delay, 0) as snow_delay,
        if(w.hail, d.dep_delay, 0) as hail_delay, if(w.thunder, d.dep_delay, 0) as thunder_delay,
        if(w.tornado,  d.dep_delay, 0) as tornado_delay,
        if(w.fog or w.rain or w.snow or w.hail or w.thunder or w.tornado, 0, d.dep_delay) as clear_delay
        from dlys d join weathersummary w
          on d.year = w.year and d.month = w.month and d.day = w.day and d.origin_code = w.station""")
    }
    // DataFrame style
    def fawDataFrameStyle() = {
      val delays = ontime.join(stations, ontime("origin") <=> stations("name")).
                select(ontime("year"), ontime("month"), ontime("dayofmonth").as("day"), 
                       ontime("origin"), ontime("dest"), stations("code"), ontime("depdelay"))
                
      // First, join the delays with the weather
      val dw = delays.join(ws, delays("code") <=> ws("station") && 
                              delays("year") <=> ws("year") &&
                              delays("month") <=> ws("month") &&
                              delays("day") <=> ws("day"))
                              
      // Now build the final column set. Notice how SQL "if" becomes "when" 
      dw.select(delays("year"), delays("month"), delays("day"), 
                delays("origin").as("origin_name"), delays("dest").as("dest_name"),
                dw("fog"), when(dw("fog"), dw("depdelay")).otherwise(0).as("fog_delay"),
                dw("rain"), when(dw("rain"), dw("depdelay")).otherwise(0).as("rain_delay"),
                dw("snow"), when(dw("snow"), dw("depdelay")).otherwise(0).as("snow_delay"),
                dw("hail"), when(dw("hail"), dw("depdelay")).otherwise(0).as("hail_delay"),
                dw("thunder"), when(dw("thunder"), dw("depdelay")).otherwise(0).as("thunder_delay"),
                dw("tornado"), when(dw("tornado"), dw("depdelay")).otherwise(0).as("tornado_delay"),
                when (dw("fog").or(dw("rain")).or(dw("snow")).or(dw("hail")).or(dw("thunder")).or(dw("tornado")), 0).otherwise(dw("depdelay")).as("clear_delay"))
    }
    
    // Comment out whichever of the following you want to use
    val faw = fawHiveStyle()
    // val faw = fawDataFrameStyle()

    // Construct route_delays DataFrame style. Exercise: Can you do it SQL style?
    val route_delays = faw.groupBy(faw("origin_name"), faw("dest_name")).
            agg(sum(when(faw("fog"), 1).otherwise(0)).as("fog_flights"), sum(faw("fog_delay")).as("fog_delays"),
                sum(when(faw("rain"), 1).otherwise(0)).as("rain_flights"), sum(faw("rain_delay")).as("rain_delays"),
                sum(when(faw("snow"), 1).otherwise(0)).as("snow_flights"), sum(faw("snow_delay")).as("snow_delays"),
                sum(when(faw("hail"), 1).otherwise(0)).as("hail_flights"), sum(faw("hail_delay")).as("hail_delays"),
                sum(when(faw("thunder"), 1).otherwise(0)).as("thunder_flights"), sum(faw("thunder_delay")).as("thunder_delays"),
                sum(when(faw("tornado"), 1).otherwise(0)).as("tornado_flights"), sum(faw("tornado_delay")).as("tornado_delays"),
                sum(when (faw("fog").or(faw("rain")).or(faw("snow")).or(faw("hail")).or(faw("thunder")).or(faw("tornado")), 0).otherwise(1)).as("clear_flights"),
                sum(faw("clear_delay")).as("clear_delays"))
    
    // Save to Hive because bulk loading HBase from Spark is awkward
    route_delays.write.mode(SaveMode.Overwrite).saveAsTable("Spark_Route_Delays")

    // On your VM only, you may need to modify the above command with the path to the table
    // Warning: Whatever path you specify will be overwritten. May be a good time to take a snapshot of your VM
    // route_delays.write.options(Map("path" -> "/usr/hive/warehouse/spark_route_delays")).mode(SaveMode.Overwrite).saveAsTable("Spark_Route_Delays")

    // To move into HBase, use the following hive script which is exactly the same as write_to_hbase.hql except with a different table name
/* create external table spark_weather_delays_by_route (
  route string,
  clear_flights bigint, clear_delays bigint,
  fog_flights bigint, avg_fog_delay bigint,
  rain_flights bigint, avg_rain_delay bigint,
  snow_flights bigint, avg_snow_delay bigint,
  hail_flights bigint, avg_hail_delay bigint,
  thunder_flights bigint, avg_thunder_delay bigint,
  tornado_flights bigint, avg_tornado_delay bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,delay:clear_flights,delay:clear_delays,delay:fog_flights,delay:fog_delays,delay:rain_flights,delay:rain_delays,delay:snow_flights,delay:snow_delays,delay:hail_flights,delay:hail_delays,delay:thunder_flights,delay:thunder_delays,delay:tornado_flights,delay:tornado_delays')
TBLPROPERTIES ('hbase.table.name' = 'spark_weather_delays_by_route');


insert overwrite table spark_weather_delays_by_route
select concat(origin_name,dest_name),
  clear_flights, clear_delays,
  fog_flights, fog_delays,
  rain_flights, rain_delays,
  snow_flights, snow_delays,
  hail_flights, hail_delays,
  thunder_flights, thunder_delays,
  tornado_flights, tornado_delays from spark_route_delays;
 
     */

 }
}
