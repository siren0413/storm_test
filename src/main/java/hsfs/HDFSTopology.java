package hsfs;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

/**
 * Created by yijunmao on 7/11/17.
 */
public class HDFSTopology {
    public static void main(String[] args) throws Exception {
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(60.0f, TimedRotationPolicy.TimeUnit.SECONDS);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/tmp/foo/")
                .withExtension(".txt");

        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");

//        Yaml yaml = new Yaml();
//        InputStream in = new FileInputStream(args[1]);
//        Map<String, Object> yamlConf = (Map<String, Object>) yaml.load(in);
//        in.close();

        Config config = new Config();
        config.setNumWorkers(1);
//        config.put("hdfs.config", yamlConf);


        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://cdh01:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .addRotationAction(new MoveFileAction().toDestination("/tmp/dest2/"));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("hdfs-spout", new HDFSSpout(), 1);
        builder.setBolt("hdfs-bolt", bolt, 4).shuffleGrouping("hdfs-spout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("hdfs-test-topology", config, builder.
                createTopology());

        Thread.sleep(100* 1000);
        cluster.killTopology("hdfs-test-topology");
        cluster.shutdown();
    }
}
