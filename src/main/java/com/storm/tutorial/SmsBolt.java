package com.storm.tutorial;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Date;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class SmsBolt extends BaseRichBolt {

  private OutputCollector outputCollector;
  private ObjectMapper objectMapper = new ObjectMapper();

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  public void execute(Tuple tuple) {
    String body = tuple.getString(0);
    try{
      DataDTO dto = objectMapper.readValue(body, DataDTO.class);
      Long time = new Date().getTime();
      System.out.println("Received data in SmsBolt:"+dto+",time:"+time);
      if(time%2 ==0){
        System.err.println("Manually throwing error:"+dto);
        outputCollector.fail(tuple);
      }
      else{
        outputCollector.ack(tuple);
      }
    }
    catch(Exception e){
      System.err.println("Some error occurred");
      e.printStackTrace();
      outputCollector.fail(tuple);
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

  }
}
