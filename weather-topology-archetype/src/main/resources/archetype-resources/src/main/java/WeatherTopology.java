#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package};

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

public class WeatherTopology {

	static class FilterAirportsBolt extends BaseBasicBolt {
		Pattern stationPattern;
		Pattern weatherPattern;
		@Override
		public void prepare(Map stormConf, TopologyContext context) {
			stationPattern = Pattern.compile("<station_id>K([A-Z]{3})</station_id>");
			weatherPattern = Pattern.compile("<weather>([^<]*)</weather>");
			super.prepare(stormConf, context);
		}

		@Override
		public void execute(Tuple tuple, BasicOutputCollector collector) {
			String report = tuple.getString(0);
			Matcher stationMatcher = stationPattern.matcher(report);
			if(!stationMatcher.find()) {
				return;
			}
			Matcher weatherMatcher = weatherPattern.matcher(report);
			if(!weatherMatcher.find()) {
				return;
			}
			collector.emit(new Values(stationMatcher.group(1), weatherMatcher.group(1)));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("airport", "weather"));
		}

	}

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

		String zkIp = "localhost";

		String nimbusHost = "sandbox.hortonworks.com";

		String zookeeperHost = zkIp +":2181";

		ZkHosts zkHosts = new ZkHosts(zookeeperHost);
		List<String> zkServers = new ArrayList<String>();
		zkServers.add(zkIp);
		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, "spertus-weather-events", "/spertus-weather-events","test_id");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
		kafkaConfig.zkServers = zkServers;
		kafkaConfig.zkRoot = "/spertus-weather-events";
		kafkaConfig.zkPort = 2181;
		kafkaConfig.forceFromStart = true;
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		TopologyBuilder builder = new TopologyBuilder();

		HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://sandbox.hortonworks.com:8020")
				.withFileNameFormat(new DefaultFileNameFormat().withPath("/tmp/test"))
				.withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("|"))
				.withSyncPolicy(new CountSyncPolicy(10))
				.withRotationPolicy(new FileSizeRotationPolicy(5.0f, Units.MB));
		builder.setSpout("raw-weather-events", kafkaSpout, 1);
		builder.setBolt("filter-airports", new FilterAirportsBolt(), 1).shuffleGrouping("raw-weather-events");
		//        builder.setBolt("test-bolt", new TestBolt(), 1).shuffleGrouping("raw-weather-events");
		//        builder.setBolt("hdfs-bolt", hdfsBolt, 1).shuffleGrouping("raw-weather-events");


		Map conf = new HashMap();
		conf.put(backtype.storm.Config.TOPOLOGY_WORKERS, 4);
		conf.put(backtype.storm.Config.TOPOLOGY_DEBUG, true);
		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}   else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("weather-topology", conf, builder.createTopology());
		}
	}
}
