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

public class EmailStreamBolt   extends BaseRichBolt {
  private OutputCollector outputCollector;
  private ObjectMapper objectMapper = new ObjectMapper();

  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    this.outputCollector = outputCollector;
  }


  public void execute(Tuple tuple) {
    String body = tuple.getString(0);
    try{
      DataDTO dto = objectMapper.readValue(body, DataDTO.class);
      System.out.println("Received data in EmailBoltStream:"+dto);
      outputCollector.emit(tuple,new Values(body));
      outputCollector.ack(tuple);
    }
    catch(Exception e){
      System.err.println("Some error occurred");
      e.printStackTrace();
//      outputCollector.fail(tuple);
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("sample"));
  }
}
