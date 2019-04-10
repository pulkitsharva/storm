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
    String topic = "bills";
    BrokerHosts hosts = new ZkHosts(zkConnString);

    SpoutConfig billsSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic, "Bills-Spout");
    billsSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
    billsSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
    billsSpoutConfig.failedMsgRetryManagerClass = ExponentialBackoffMsgRetryManager.class.getName();
    billsSpoutConfig.retryInitialDelayMs = 100;
    billsSpoutConfig.retryDelayMultiplier = 1.0;
    billsSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("Kafka-Spout", new KafkaSpout(billsSpoutConfig));
    builder.setBolt("AccountsBolt", new AccountsBolt()).shuffleGrouping("Kafka-Spout");
    builder.setBolt("EmailBolt", new EmailBolt()).allGrouping("AccountsBolt", "smsStr" + "eam");
    builder.setBolt("SmsBolt", new SmsBolt()).allGrouping("AccountsBolt", "emailStream");

    
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("BillsTopology", config, builder.createTopology());
  }
}
