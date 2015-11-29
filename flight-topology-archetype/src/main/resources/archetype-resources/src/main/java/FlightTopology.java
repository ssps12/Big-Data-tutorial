#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package};

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

public class FlightTopology {

	static class GetFlightStatsBolt extends BaseBasicBolt {
		ObjectMapper mapper;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			mapper = new ObjectMapper();
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String flightJSON = tuple.getString(0);
			FlightInfoExResponse.FlightInfoExResult.Flight flight;
			try {
				flight = mapper.readValue(flightJSON, FlightInfoExResponse.class).getFlightInfoExResult().getFlights()[0];
				int delay = (flight.getActualdeparturetime() - flight.getFiledDeparturetime())/60;
				collector.emit(new Values(flight.getOrigin().substring(1), // Convert KORD to ORD
						                  flight.getDestination().substring(1),
						                  delay));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("origin", "destination", "delay"));
		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		String zkIp = "localhost";

		String nimbusHost = "sandbox.hortonworks.com";

		String zookeeperHost = zkIp +":2181";

		ZkHosts zkHosts = new ZkHosts(zookeeperHost);
		List<String> zkServers = new ArrayList<String>();
		zkServers.add(zkIp);
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "spertus-flight-events", "/spertus-flights-events","flight_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		kafkaConfig.zkServers = zkServers;
		kafkaConfig.zkRoot = "/spertus-flight-events";
		kafkaConfig.zkPort = 2181;
		kafkaConfig.forceFromStart = true;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("flight-events", kafkaSpout, 1);
		builder.setBolt("flight-stats", new GetFlightStatsBolt(), 1).shuffleGrouping("flight-events");

		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);
		conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("flight-topology", conf, builder.createTopology());
		}
	}
}
