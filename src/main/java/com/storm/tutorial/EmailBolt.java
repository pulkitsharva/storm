package com.storm.tutorial;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class EmailBolt implements IRichBolt {

  private ObjectMapper objectMapper = new ObjectMapper();
  private OutputCollector outputCollector;
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  public void execute(Tuple tuple) {
    String body = tuple.getString(0);
    try{
      DataDTO dto = objectMapper.readValue(body, DataDTO.class);
      System.out.println("Received data in EmailBolt:"+dto);
      outputCollector.ack(tuple);
    }
    catch(Exception e){
      System.err.println("Some error occurred");
      e.printStackTrace();
    }

  }

  public void cleanup() {

  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }

  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
