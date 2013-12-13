package com.flume.storm.spout;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.SourceRunner;
import org.apache.flume.Transaction;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.node.MaterializedConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.flume.storm.common.Constants;
import com.flume.storm.common.MaterializedConfigurationProvider;
import com.flume.storm.common.StormEmbeddedAgentConfiguration;
import com.flume.storm.producer.TupleProducer;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author Ravikumar Visweswara
 */

@SuppressWarnings("serial")
public class FlumeSpout implements IRichSpout {

	private static final Logger LOG = LoggerFactory
			.getLogger(FlumeSpout.class);
	
	public static final String DEFAULT_FLUME_PROPERTY_PREFIX = "flume-agent";
	public static final String FLUME_AGENT_NAME = "flume-agent";
	public static final String FLUME_BATCH_SIZE = "batch-size";
	public static final int DEFAULT_BATCH_SIZE = 100;

	private Channel channel;
	private MaterializedConfigurationProvider configurationProvider;
	private SourceRunner sourceRunner;
	private SinkCounter sinkCounter;

	private int batchSize = DEFAULT_BATCH_SIZE;

	private SpoutOutputCollector collector;

	private ConcurrentHashMap<String, Event> pendingMessages;
	
    private String flumePropertyPrefix = DEFAULT_FLUME_PROPERTY_PREFIX;

    public String getFlumePropertyPrefix() {
		return flumePropertyPrefix;
	}

	public void setFlumePropertyPrefix(String flumePropertPrefix) {
		this.flumePropertyPrefix = flumePropertPrefix;
	}

	public SpoutOutputCollector getCollector() {
		return collector;
	}

	public ConcurrentHashMap<String, Event> getPendingMessages() {
		return pendingMessages;
	}

	private TupleProducer tupleProducer;

	public TupleProducer getTupleProducer() {
		return tupleProducer;
	}

	public void setTupleProducer(TupleProducer tupleProducer) {
		this.tupleProducer = tupleProducer;
	}

	@SuppressWarnings("rawtypes")
	public void open(Map config, TopologyContext context,
			SpoutOutputCollector collector) {

		this.configurationProvider = new MaterializedConfigurationProvider();
		
		Map<String, String> flumeAgentProps = Maps.newHashMap();
		for (Object key : config.keySet()) {
			LOG.debug("FlumeSpout conf property:" + key.toString() + ":" + config.get(key));
			if (key.toString().startsWith(getFlumePropertyPrefix())) {
				if (key.toString().contains(FLUME_BATCH_SIZE)) {
					String batchSizeStr = (String) config.get(key);
					try {
						this.batchSize = Integer.parseInt(batchSizeStr);
					} catch (Exception e) {
						// tolerate this error and default it to the default
						// batch size
						this.batchSize = DEFAULT_BATCH_SIZE;
					}
				} else {
					flumeAgentProps.put(
							key.toString().replace(getFlumePropertyPrefix() + ".",
									""), (String) config.get(key));
				}
			}
		}

		flumeAgentProps = StormEmbeddedAgentConfiguration.configure(
				FLUME_AGENT_NAME, flumeAgentProps);
		MaterializedConfiguration conf = configurationProvider.get(
				getFlumePropertyPrefix(), flumeAgentProps);

		Map<String, Channel> channels = conf.getChannels();
		if (channels.size() != 1) {
			throw new FlumeException("Expected one channel and got "
					+ channels.size());
		}
		Map<String, SourceRunner> sources = conf.getSourceRunners();
		if (sources.size() != 1) {
			throw new FlumeException("Expected one source and got "
					+ sources.size());
		}

		this.sourceRunner = sources.values().iterator().next();
		this.channel = channels.values().iterator().next();

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(FlumeSpout.class.getName());
		}

		if (null == this.getTupleProducer()) {
			throw new IllegalStateException("Tuple Producer has not been set.");
		}

		this.collector = collector;
		this.pendingMessages = new ConcurrentHashMap<String, Event>();

		try {
			this.start();
		} catch (Exception e) {
			LOG.warn("Error Starting Flume Source/channel", e);
		}
	}

	/*
	 * Starts all the flume components 
	 */
	private void start() {
		if (null == this.sourceRunner || null == this.channel) {
			throw new FlumeException(
					"Source/Channel is null. Cannot start flume components");
		}
		this.sourceRunner.start();
		this.channel.start();
		this.sinkCounter.start();
	}

	/*
	 * Stops all the flume components
	 */
	private void stop() {
		if (null == this.sourceRunner || null == this.channel) {
			return;
		}
		this.sourceRunner.stop();
		this.channel.stop();
		this.sinkCounter.stop();
	}

	/*
	 * On close, Flume components needs to be stopped
	 * 
	 * @see backtype.storm.spout.ISpout#close()
	 */
	public void close() {
		try {
			this.stop();
		} catch (Exception e) {
			LOG.warn("Error closing Avro RPC server.", e);
		}
	}

	/*
	 * Since FlumeSource, Channel is not deactivated, dont need to activate
	 * FlumeSource, Channel
	 * 
	 * @see backtype.storm.spout.ISpout#activate()
	 */
	public void activate() {
		// TODO Auto-generated method stub
	}

	/*
	 * Currently No support for deactivation as memory channel messages will be lost.
	 * Depends on case to case. This functionality needs to be extended
	 * 
	 * @see backtype.storm.spout.ISpout#deactivate()
	 */
	public void deactivate() {
		// TODO Auto-generated method stub
	}

	public void nextTuple() {

		//Transaction Begins
		Transaction transaction = channel.getTransaction();
		int size = 0;
		try {
			transaction.begin();
			List<Event> batch = Lists.newLinkedList();
			
			//Get all the messages upto the batch size into memory
			for (int i = 0; i < this.batchSize; i++) {
				Event event = channel.take();
				if (event == null) {
					break;
				}
				batch.add(event);
			}

			//Update the counters if the batch is empty or underFlow
			size = batch.size();
			if (size == 0) {
				sinkCounter.incrementBatchEmptyCount();
			} else {
				if (size < this.batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}
				sinkCounter.addToEventDrainAttemptCount(size);
			}

			// Emit all the the events in the topology before committing the
			// transaction
			for (Event event : batch) {
				
				Values vals = this.getTupleProducer().toTuple(event);
				this.collector.emit(vals);
				this.pendingMessages.put(
						event.getHeaders().get(Constants.MESSAGE_ID), event);
				
				LOG.debug("NextTuple:"
						+ event.getHeaders().get(Constants.MESSAGE_ID));
			}
			
			//If Everything went fine, Commit the transaction 
			transaction.commit();
			sinkCounter.addToEventDrainSuccessCount(size);

		} catch (Throwable t) {
			//On error, roll back the whole batch
			transaction.rollback();
			if (t instanceof Error) {
				throw (Error) t;
			} else if (t instanceof ChannelException) {
				LOG.error(
						"Unable to get event from" + " channel "
								+ channel.getName() + ". Exception follows.", t);
			} else {
				LOG.error("Failed to emit events", t);
			}
		} finally {
			transaction.close();
		}

		//if we come across empty batch, sleep for some time as the load is not that high anyways
		if (size == 0) {
			Utils.sleep(100);
		}
	}

	/*
	 * When a message is succeeded remove from the pending list
	 * 
	 * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
	 */
	public void ack(Object msgId) {
		this.pendingMessages.remove(msgId);
	}

	/*
	 * When a message fails, retry the message by pushing the event back to channel.
	 * Note: Please test this situation...
	 * 
	 * @see backtype.storm.spout.ISpout#fail(java.lang.Object)
	 */
	public void fail(Object msgId) {
		//on a failure, push the message from pending to flume channel;
		
		Event ev = this.pendingMessages.get(msgId);
		if(null != ev){
			this.channel.put(ev);
		}
	}

	/*
	 * Component configuration
	 * 
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	/*
	 * declaration of fields will be delegated to the Tuple producer
	 * 
	 * @see
	 * backtype.storm.topology.IComponent#declareOutputFields(backtype.storm
	 * .topology.OutputFieldsDeclarer)
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		this.getTupleProducer().declareOutputFields(declarer);
	}
}
