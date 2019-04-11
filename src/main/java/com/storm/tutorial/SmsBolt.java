package com.storm.tutorial;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Date;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SmsBolt extends BaseRichBolt {

  private OutputCollector outputCollector;
  private ObjectMapper objectMapper = new ObjectMapper();

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  public void execute(Tuple tuple) {
    String body = tuple.getString(0);
    try{
      AccountDTO dto = objectMapper.readValue(body, AccountDTO.class);
      System.out.println("Received data in SmsBolt:"+dto);
      if(dto.getShouldFailAtSms()){
        this.outputCollector.fail(tuple);
      }
      else {
        this.outputCollector.emit(new Values(body,"random"));
        this.outputCollector.ack(tuple);

      }
    }
    catch(Exception e){
      System.err.println("Some error occurred");
      e.printStackTrace();
      outputCollector.fail(tuple);
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("key","word"));
  }
}
