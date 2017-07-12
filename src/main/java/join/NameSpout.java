package join;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by yijunmao on 7/10/17.
 */
public class NameSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;

    private int id = 0;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "name"));
    }
    public void open(Map config, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
    }
    public void nextTuple() {
        while (true) {
            this.collector.emit(new Values(id, "name" + id));
            id ++;
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }



}