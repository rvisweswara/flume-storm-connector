package com.flume.storm.producer;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.flume.storm.common.Constants;

import backtype.storm.tuple.Tuple;

/**
 * @author Ravikumar Visweswara
 */

@SuppressWarnings("serial")
public class SimpleFlumeEventProducer implements FlumeEventProducer {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleFlumeEventProducer.class);
	
	private static final String CHARSET = "UTF-8";
	
	public static String getCharset() {
		return CHARSET;
	}

	@SuppressWarnings("unchecked")
	public Event toEvent(Tuple input) throws Exception {
		
		Map<String, String> headers = null;
		Object headerObj = null;
		Object messageObj = null;
		String messageStr = null;
		
		/*If the number of parameters are two, they are assumed as MessageId and Message
		 *If the number of parameters are three, they are assumed as MessageId, Headers and Message
		 */
		if(input.size()==2){
			messageObj = input.getValue(1);
			headers = new HashMap<String, String>();
			headers.put(Constants.MESSAGE_ID, input.getString(0));
			headers.put(Constants.TIME_STAMP, String.valueOf(System.currentTimeMillis()));
		}else if(input.size()==3){
			headerObj = input.getValue(1);
			messageObj = input.getValue(2);
			headers = (Map<String, String>)headerObj;	
			
			LOG.debug("String format of object:" +  ((String)input.getValue(2)));
		}else{
			throw new IllegalStateException("Wrong format of touple expected 2 or 3 values. But found " + input.size());
		}
		
		try {
			messageStr = (String)messageObj;
		   
			LOG.debug("SimpleAvroFlumeEventProducer:MSG:" + messageStr);		    
			
			Event event = EventBuilder.withBody(messageStr.getBytes(), headers);
			return event;
		} catch (Exception e){
			throw e;
		}
	}

}
