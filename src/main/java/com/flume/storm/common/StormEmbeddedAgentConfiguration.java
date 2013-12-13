package com.flume.storm.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flume.FlumeException;
import org.apache.flume.agent.embedded.EmbeddedSource;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.apache.flume.conf.channel.ChannelType;
import org.apache.flume.conf.sink.SinkProcessorType;
import org.apache.flume.conf.sink.SinkType;
import org.apache.flume.conf.source.SourceType;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
/**
 * @author Ravikumar Visweswara
 */

/**
 * Stores publicly accessible configuration constants and private
 * configuration constants and methods.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class StormEmbeddedAgentConfiguration {
  public static final String SEPERATOR = ".";
  private static final Joiner JOINER = Joiner.on(SEPERATOR);
  private static final String TYPE = "type";

  /**
   * Prefix for source properties
   */
  public static final String SOURCE = "source";
  /**
   * Prefix for channel properties
   */
  public static final String CHANNEL = "channel";
  /**
   * Source type, choices are Avro 
   */
  public static final String SOURCE_TYPE = join(SOURCE, TYPE);
  /**
   * Prefix for passing configuration parameters to the source
   */
  public static final String SOURCE_PREFIX = join(SOURCE, "");
  /**
   * Channel type, choices are `memory' or `file'
   */
  public static final String CHANNEL_TYPE = join(CHANNEL, TYPE);
  /**
   * Prefix for passing configuration parameters to the channel
   */
  public static final String CHANNEL_PREFIX = join(CHANNEL, "");
  /**
   * Memory channel which stores events in heap. See Flume User Guide for
   * configuration information. This is the recommended channel to use for
   * Embedded Agents.
   */
  public static final String CHANNEL_TYPE_MEMORY = ChannelType.MEMORY.name();
  /**
   * File based channel which stores events in on local disk. See Flume User
   * Guide for configuration information.
   */
  public static final String CHANNEL_TYPE_FILE = ChannelType.FILE.name();
  
  /**
   * Avro source which can receive events from another avro sink or client. 
   */
  public static final String SOURCE_TYPE_AVRO = SourceType.AVRO.name();

  private static final String[] ALLOWED_SOURCES = {
    SOURCE_TYPE_AVRO
  };

  private static final String[] ALLOWED_CHANNELS = {
    CHANNEL_TYPE_MEMORY,
    CHANNEL_TYPE_FILE
  };

  /*
   * Validates the source and channel configuration for required properties
   */
  private static void validate(String name,
      Map<String, String> properties) throws FlumeException {

    if(properties.containsKey(SOURCE_TYPE)) {
      checkAllowed(ALLOWED_SOURCES, properties.get(SOURCE_TYPE));
    }
    checkRequired(properties, CHANNEL_TYPE);
    checkAllowed(ALLOWED_CHANNELS, properties.get(CHANNEL_TYPE));
  }
  /**
   * Folds Storm embedded configuration structure into an agent configuration.
   * Should only be called after validate returns without error.
   *
   * @param name - agent name
   * @param properties - storm embedded agent configuration
   * @return configuration applicable to a flume agent
   */
  public static Map<String, String> configure(String name,
      Map<String, String> properties) throws FlumeException {
    validate(name, properties);
    // we are going to modify the properties as we parse the config
    properties = new HashMap<String, String>(properties);

    if(!properties.containsKey(SOURCE_TYPE) || SOURCE_TYPE_AVRO.
        equalsIgnoreCase(properties.get(SOURCE_TYPE))) {
      properties.put(SOURCE_TYPE, SOURCE_TYPE_AVRO);
    }
    
    String sourceName = "source-" + name;
    String channelName = "channel-" + name;

    /*
     * Now we are going to process the user supplied configuration
     * and generate an agent configuration. This is only to supply
     * a simpler client api than passing in an entire agent configuration.
     */
    // user supplied config -> agent configuration
    Map<String, String> result = Maps.newHashMap();

    // properties will be modified during iteration so we need a
    // copy of the keys
    Set<String> userProvidedKeys;
    /*
     * First we are going to setup all the root level pointers. I.E
     * point the agent at the components and
     * source at the channel.
     */
    // point agent at source
    result.put(join(name, BasicConfigurationConstants.CONFIG_SOURCES),
        sourceName);
    // point agent at channel
    result.put(join(name, BasicConfigurationConstants.CONFIG_CHANNELS),
        channelName);
    
    result.put(join(name,
        BasicConfigurationConstants.CONFIG_SOURCES, sourceName,
        BasicConfigurationConstants.CONFIG_CHANNELS), channelName);
    
    /*
     * Third, process all remaining configuration items, prefixing them
     * correctly and then passing them on to the agent.
     */
    userProvidedKeys = new HashSet<String>(properties.keySet());
    for(String key : userProvidedKeys) {
      String value = properties.get(key);
      if(key.startsWith(SOURCE_PREFIX)) {
        // users use `source' but agent needs the actual source name
        key = key.replaceFirst(SOURCE, sourceName);
        result.put(join(name,
            BasicConfigurationConstants.CONFIG_SOURCES, key), value);
      } else if(key.startsWith(CHANNEL_PREFIX)) {
        // users use `channel' but agent needs the actual channel name
        key = key.replaceFirst(CHANNEL, channelName);
        result.put(join(name,
            BasicConfigurationConstants.CONFIG_CHANNELS, key), value);
      } else {
        // We may want to ignore this
        throw new FlumeException("Unknown/Unsupported configuration " + key);
      }
    }
    
    System.out.println("result:" + result.toString());
    
    return result;
  }
  private static void checkAllowed(String[] allowedTypes, String type) {
    boolean isAllowed = false;
    type = type.trim();
    for(String allowedType : allowedTypes) {
      if(allowedType.equalsIgnoreCase(type)) {
        isAllowed = true;
        break;
      }
    }
    if(!isAllowed) {
      throw new FlumeException("Component type of " + type + " is not in " +
          "allowed types of " + Arrays.toString(allowedTypes));
    }
  }
  private static void checkRequired(Map<String, String> properties,
      String name) {
	  System.out.println(name + ":" + properties.toString());
    if(!properties.containsKey(name)) {
      throw new FlumeException("Required parameter not found " + name);
    }
  }

  private static String join(String... parts) {
    return JOINER.join(parts);
  }

  private StormEmbeddedAgentConfiguration() {

  }
}
