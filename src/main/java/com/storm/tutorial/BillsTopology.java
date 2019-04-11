package com.storm.tutorial;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ExponentialBackoffMsgRetryManager;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
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


    Properties kafka = new Properties();

    kafka.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    kafka.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    kafka.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    kafka.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "SMS-Spout");
    kafka.setProperty("request.required.acks", "1");


    KafkaBolt bolt = new KafkaBolt().withProducerProperties(kafka)

        .withTopicSelector(new DefaultTopicSelector("sms"))

        .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper("key", "word"));
    builder.setBolt("Sms-Spout",bolt).allGrouping("SmsBolt");

    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("BillsTopology", config, builder.createTopology());
  }
}
