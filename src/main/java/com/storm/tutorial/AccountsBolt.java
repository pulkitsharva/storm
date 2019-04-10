package com.storm.tutorial;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class AccountsBolt extends BaseRichBolt {

  private OutputCollector outputCollector;
  private ObjectMapper objectMapper = new ObjectMapper();

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }

  public void execute(Tuple tuple) {
    String body = tuple.getString(0);
    try{
      AccountDTO dto = objectMapper.readValue(body, AccountDTO.class);
      System.out.println("Received data in BillsBolt:"+dto);
      if (dto.getShouldFailAtAccount()) {
        outputCollector.fail(tuple);
      }
      else{
        outputCollector.emit("smsStream", new Values(body));
        outputCollector.emit("emailStream",new Values(body));
        outputCollector.ack(tuple);
      }
    }
    catch(Exception e){
      System.err.println("Some error occurred");
      e.printStackTrace();
//      outputCollector.fail(tuple);
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declareStream("smsStream", new Fields("sample"));
    outputFieldsDeclarer.declareStream("emailStream", new Fields("sample"));
  }
}
