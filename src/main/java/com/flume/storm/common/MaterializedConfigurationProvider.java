package com.flume.storm.common;

import java.util.Map;

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.node.MaterializedConfiguration;

/**
 * @author Ravikumar Visweswara
 */

/**
 * Provides {@see MaterializedConfiguration} for a given agent and set of
 * properties. This class exists simply to make more easily testable. That is
 * it allows us to mock the actual Source, Sink, and Channel components
 * as opposed to instantiation of real components.
 */
public class MaterializedConfigurationProvider {

  public MaterializedConfiguration get(String name, Map<String, String> properties) {
    MemoryConfigurationProvider confProvider =
        new MemoryConfigurationProvider(name, properties);
    return confProvider.getConfiguration();
  }
}
