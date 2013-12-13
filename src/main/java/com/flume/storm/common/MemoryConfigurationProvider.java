package com.flume.storm.common;

import java.util.Map;

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.node.AbstractConfigurationProvider;

/**
 * @author Ravikumar Visweswara
 */

/**
 * MemoryConfigurationProvider is the simplest possible
 * AbstractConfigurationProvider simply turning a give properties file and
 * agent name into a FlumeConfiguration object.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class MemoryConfigurationProvider extends AbstractConfigurationProvider {
  private final Map<String, String> properties;

  MemoryConfigurationProvider(String name, Map<String, String> properties) {
    super(name);
    this.properties = properties;
  }

  @Override
  protected FlumeConfiguration getFlumeConfiguration() {
    return new FlumeConfiguration(properties);
  }

}
