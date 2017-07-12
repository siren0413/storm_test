package join;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by yijunmao on 7/11/17.
 */
public class FilterBolt extends BaseWindowedBolt {

    private Set<Integer> visited = new HashSet<Integer>();

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(TupleWindow inputWindow) {
        List<Tuple> tuples = inputWindow.get();
        Set<Integer> temp = new HashSet<Integer>();
        for (Tuple tuple: tuples) {
            if (!visited.contains(tuple.getIntegerByField("nameSpout:id"))){
                System.out.println(tuple);
//                this.collector.emit(tuple.getValues());
            }else {
                System.out.println("duplicate");
            }

            temp.add(tuple.getIntegerByField("nameSpout:id"));
            visited.add(tuple.getIntegerByField("nameSpout:id"));
        }
        visited = temp;
    }
}
