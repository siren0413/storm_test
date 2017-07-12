package join;

import com.chris.ReportBolt;
import com.chris.SentenceSpout;
import com.chris.SplitSentenceBolt;
import com.chris.WordCountBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.bolt.JoinBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import java.util.concurrent.TimeUnit;

/**
 * Created by yijunmao on 7/10/17.
 */
public class JoinTopology {

    public static void main(String[] args) throws Exception {


        GenderSpout genderSpout = new GenderSpout();
        NameSpout nameSpout = new NameSpout();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("nameSpout", nameSpout);
        builder.setSpout("genderSpout", genderSpout);

        JoinBolt joiner = new JoinBolt("nameSpout", "id")
                .join("genderSpout",    "id", "nameSpout")
                .select ("nameSpout:id,genderSpout:id,name,gender")
//                .withTumblingWindow(new BaseWindowedBolt.Duration(3, TimeUnit.SECONDS));
                .withWindow(new BaseWindowedBolt.Duration(5, TimeUnit.SECONDS), new BaseWindowedBolt.Duration(4, TimeUnit.SECONDS));

        builder.setBolt("joiner", joiner)
                .fieldsGrouping("nameSpout", new Fields("id"))
                .fieldsGrouping("genderSpout", new Fields("id"));

        builder.setBolt("filter", new FilterBolt().withTumblingWindow(new BaseWindowedBolt.Duration(6, TimeUnit.SECONDS)))
                .fieldsGrouping("joiner", new Fields("nameSpout:id"));

//        builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("filter");

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test-topology", config, builder.
                createTopology());
        Thread.sleep(100* 1000);
        cluster.killTopology("test-topology");
        cluster.shutdown();
    }
}
