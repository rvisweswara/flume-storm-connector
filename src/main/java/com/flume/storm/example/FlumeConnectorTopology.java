package com.flume.storm.example;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.flume.agent.embedded.EmbeddedAgent;
import org.apache.flume.event.EventBuilder;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.flume.storm.producer.HeaderTextMessageTupleProducer;
import com.flume.storm.spout.FlumeSpout;
import com.google.common.collect.Maps;

public class FlumeConnectorTopology {
	public static final String FLUME_SPOUT = "FLUME_SPOUT";
	public static final String FAKE_BOLT = "FAKE_BOLT";

	public static void main(String[] args) throws Exception {

		String propertyFile = "topology.proporties";

		InputStream propInputStream = new FileInputStream(propertyFile);
        Properties props = new Properties();
        props.load(propInputStream);
        
        HeaderTextMessageTupleProducer producer = new HeaderTextMessageTupleProducer();
		FlumeSpout flumeSpout = new FlumeSpout();
		flumeSpout.setTupleProducer(producer);
		flumeSpout.setFlumePropertyPrefix("flume-agent");

		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(FLUME_SPOUT, flumeSpout, 1);

		builder.setBolt(FAKE_BOLT,
				new GenericBolt("FAKE_BOLT", true, true, new Fields("message")), 3).shuffleGrouping(
						FLUME_SPOUT);
		
		Config conf = new Config();

        conf.setDebug(true);
        conf.setNumWorkers(3);
        conf.setStatsSampleRate(1);
        conf.setMaxSpoutPending(200);
        conf.setMessageTimeoutSecs(60);
        
        for (@SuppressWarnings("rawtypes")
        Entry e : props.entrySet()) {
            if (e.getKey() instanceof String) {
                conf.put((String) e.getKey(), e.getValue());
            }
        }
		if (args.length > 0) {
			conf.setNumWorkers(3);

			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setDebug(true);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("flume-storm-connector", conf, builder.createTopology());
			Utils.sleep(10000);
			
			//Once topology is submitted, send events using Embedded agent. Just for fun, use load_balance processor.
			Map<String, String> properties;
			properties = Maps.newHashMap();
			properties.put("channel.type", "memory");
			properties.put("channel.capacity", "200");
			properties.put("sinks", "sink1 sink2");
			properties.put("sink1.type", "avro");
			properties.put("sink2.type", "avro");
			properties.put("sink1.hostname", "0.0.0.0");
			properties.put("sink1.port", String.valueOf(props.get("flume-agent.source.port")));
			properties.put("sink2.hostname", "0.0.0.0");
			properties.put("sink2.port", String.valueOf(props.get("flume-agent.source.port")));
			properties.put("processor.type", "load_balance");
			
			EmbeddedAgent agent = new EmbeddedAgent("test-1");
			agent.configure(properties);
			agent.start();
			for (int i=0;i<10;i++){
				String bodyStr = "mesage count=" + i;
				agent.put(EventBuilder.withBody(bodyStr.getBytes(), null));
			}
			
			Utils.sleep(600000);
			cluster.killTopology("flume-storm-connector");
			cluster.shutdown();
		}
	}

}
