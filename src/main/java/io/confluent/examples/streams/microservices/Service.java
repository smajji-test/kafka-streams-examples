package io.confluent.examples.streams.microservices;

import java.util.Optional;
import java.util.Properties;

public interface Service {

  void start(String bootstrapServers, String stateDir, Properties defaultConfig);

  void stop();
}