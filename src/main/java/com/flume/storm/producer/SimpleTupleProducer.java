package com.flume.storm.producer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.flume.Event;

import com.flume.storm.common.Constants;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/**
 * @author Ravikumar Visweswara
 */

@SuppressWarnings("serial")
public class SimpleTupleProducer implements TupleProducer{

	public Values toTuple(Event event) throws Exception {
		
		String msgID = null;
		Map<String,String> evHeaders = event.getHeaders();
		
		if(null != evHeaders){
			msgID = evHeaders.get(Constants.MESSAGE_ID);
		}else {
			evHeaders = new HashMap<String,String>();
		}
		
		//if MessageID header doesnt exists, set the MessageId
		if(null == msgID) {
			UUID randMsgID = UUID.randomUUID();
			msgID = randMsgID.toString();
			event.getHeaders().put(Constants.MESSAGE_ID, msgID);
		}
		String msg = new String(event.getBody());
		return new Values(msgID,msg);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(Constants.MESSAGE_ID,Constants.MESSAGE));
	}

}
