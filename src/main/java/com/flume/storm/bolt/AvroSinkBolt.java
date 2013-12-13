package com.flume.storm.bolt;

/**
 * @author Ravikumar Visweswara
 */
import java.util.Map;
import java.util.Properties;

import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flume.storm.producer.FlumeEventProducer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class AvroSinkBolt implements IRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSinkBolt.class);
    public static final String FLUME_PROPERTY_PREFIX_DEFAULT = "flume-sink";
    
    private String flumePropertyPrefix = FLUME_PROPERTY_PREFIX_DEFAULT;
    private Properties flumeSinkProperties;

    private OutputCollector collector;
    
    private RpcClient rpcClient;
    private FlumeEventProducer flumeEventProducer; 

    public String getFlumePropertyPrefix() {
		return flumePropertyPrefix;
	}

	public void setFlumePropertyPrefix(String flumePropertPrefix) {
		this.flumePropertyPrefix = flumePropertPrefix;
	}

	public FlumeEventProducer getFlumeEventProducer() {
        return flumeEventProducer;
    }

    public void setFlumeEventProducer(FlumeEventProducer producer) {
        this.flumeEventProducer = producer;
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map config, TopologyContext context, OutputCollector collector) {
    	
        this.collector = collector;
        flumeSinkProperties = new Properties();
        LOG.info("Looking for flume properties");
		for (Object key : config.keySet()) {
			if (key.toString().startsWith(this.getFlumePropertyPrefix())) {
				
				LOG.debug("Found:Key:" + key.toString() + ":" + (String) config.get(key));
				
				flumeSinkProperties.put(
							key.toString().replace(this.getFlumePropertyPrefix() + ".",
									""), (String) config.get(key));
			}
		}
		
        try {
            createConnection();
            LOG.info("Connected to flume hosts(s)");
        } catch (Exception e) {
            LOG.error("Error connecting to flume", e);
            destroyConnection();
        }
    }

    public void execute(Tuple input) {

        try {
            verifyConnection();
            
            Event event = this.flumeEventProducer.toEvent(input);
            
            LOG.debug("Event: " + event.toString());
            
            if (event != null) {
                this.rpcClient.append(event);
                this.collector.ack(input);
            }
        } catch (Exception e) {
            LOG.warn("Failed Tuple: " + input, e);
            this.collector.fail(input);
        }
    }

    @Override
    public void cleanup() {
    	LOG.info("Executing cleanup");
    	destroyConnection();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private void createConnection() throws Exception {
        if (rpcClient == null) {
            
        	LOG.info("Avro Sink Bolt: Building RpcClient with properties:" + flumeSinkProperties.toString());
            
        	if(this.flumeSinkProperties == null){
            	throw new FlumeException("Flume avro Source properties are found");
            }
            
        	this.rpcClient = RpcClientFactory.getInstance(this.flumeSinkProperties);
        }
    }

    private void destroyConnection() {
        if (rpcClient != null) {
            LOG.info("Avro Sink Bolt: Closing RpcClient");
            try {
                this.rpcClient.close();
            } catch (Exception e) {
                LOG.error("Error closing RpcClient. Ignoring .. Anyways its a deploy");
            }
            rpcClient = null;
            LOG.info("Closed RpcClient");
        }
    }

    private void verifyConnection() throws Exception {
        try {
            if (rpcClient == null) {
                createConnection();
            } else if (!rpcClient.isActive()) {
                destroyConnection();
                createConnection();
            }
        } catch (Exception e) {
            LOG.error("Error checking connection of RpcClient. Ignoring for now, but will fail in append");
        }
    }
}
