package com.storm.tutorial;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ExponentialBackoffMsgRetryManager;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class BillsTopology {

  public static void main(String[] args) {
    Config config = new Config();
    config.setDebug(false);
    String zkConnString = "localhost:2181";
    String topic = "storm-tutorial";
    BrokerHosts hosts = new ZkHosts(zkConnString);


    SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,
        "First-Spout");
    kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
    kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
    kafkaSpoutConfig.failedMsgRetryManagerClass = ExponentialBackoffMsgRetryManager.class.getName();
    kafkaSpoutConfig.retryInitialDelayMs = 100;
    kafkaSpoutConfig.retryDelayMultiplier=1.0;
    kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());





    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("Kafka-Spout", new KafkaSpout(kafkaSpoutConfig));
    builder.setBolt("BillsBolt", new BillsBolt()).shuffleGrouping("Kafka-Spout");
    builder.setBolt("SmsBolt", new SmsBolt()).allGrouping("BillsBolt");
    builder.setBolt("EmailBolt", new EmailBolt()).allGrouping("BillsBolt");

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("TestingTopology", config, builder.createTopology());
  }
}
