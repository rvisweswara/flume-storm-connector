package com.flume.storm.producer;

import java.io.Serializable;

import org.apache.flume.Event;

import backtype.storm.tuple.Values;
import backtype.storm.topology.OutputFieldsDeclarer;

/**
 * @author Ravikumar Visweswara
 */

public interface TupleProducer extends Serializable{
	Values toTuple(Event event) throws Exception;
	void declareOutputFields(OutputFieldsDeclarer declarer);
}
