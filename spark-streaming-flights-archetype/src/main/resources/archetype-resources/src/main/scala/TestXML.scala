import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule


object TestXML {
      def main(args: Array[String]) = {
        val foo = new String("{\"originName\":\"KJS\",\"destinationName\":\"MIA\",\"departureDelay\":7}")
        val mapper = new ObjectMapper()
        mapper.registerModule(DefaultScalaModule)    
        val kfr = mapper.readValue(foo, classOf[KafkaFlightRecord])
        println(kfr)
      }
}