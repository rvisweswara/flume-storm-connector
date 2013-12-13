package com.flume.storm.example;

import java.util.Map;

import backtype.storm.topology.base.BaseRichBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class GenericBolt extends BaseRichBolt {
	private static final Logger LOG = LoggerFactory.getLogger(GenericBolt.class);
	private OutputCollector collector;
	private boolean autoAck = false;
	private boolean autoAnchor = false;
	private Fields declaredFields;
	private String name;
	
	public GenericBolt(String name, boolean autoAck, boolean autoAnchor, Fields declaredFields){
		this.name = name;
		this.autoAck = autoAck;
		this.autoAnchor = autoAnchor;
		this.declaredFields = declaredFields;
	}
	
	public GenericBolt(String name, boolean autoAck, boolean autoAnchor){
		this(name, autoAck, autoAnchor, null);
	}

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		
	}

	public void execute(Tuple input) {
		LOG.info("[" + this.name + "] Received message: " + input);

		// only emit if we have declared fields.
		if(this.declaredFields != null){
			LOG.debug("[" + this.name + "] emitting: " + input);
			if(this.autoAnchor){
				this.collector.emit(input, input.getValues());
			} else{
				this.collector.emit(input.getValues());
			}
		}
		
		if(this.autoAck){
			LOG.debug("[" + this.name + "] ACKing tuple: " + input);
			this.collector.ack(input);
		}
		
	}

	public void cleanup() {

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		if(this.declaredFields != null){
			declarer.declare(this.declaredFields);
		}
	}
	
	public boolean isAutoAck(){
		return this.autoAck;
	}
	
	public void setAutoAck(boolean autoAck){
		this.autoAck = autoAck;
	}

}
