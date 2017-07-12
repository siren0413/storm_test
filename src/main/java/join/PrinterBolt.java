package join;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by yijunmao on 7/10/17.
 */
public class PrinterBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}