import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule


object DemoJSON {
      def main(args: Array[String]) = {
        // Deserializing JSON
        val foo = new String("{\"originName\":\"KJS\",\"destinationName\":\"MIA\",\"departureDelay\":7}")
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)  
        val kfr = mapper.readValue(foo, classOf[KafkaFlightRecord])
        
        // Serializing JSON
        mapper.writeValue(System.out, WeatherReport("ORD", true, false, false, false, false, false))
        println(kfr)
        
        // Doing a "get" from HBase is demonstrated in StreamFlights
        
      }
}