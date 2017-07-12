package hsfs;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.hdfs.common.rotation.MoveFileAction;
import org.apache.storm.kafka.spout.Func;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;
import java.util.List;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;

/**
 * Created by yijunmao on 7/11/17.
 */
public class HDFSTopology {
    public static void main(String[] args) throws Exception {
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(100.0f, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat()
                .withPath("/tmp/foo/")
                .withExtension(".log");

        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat()
                .withFieldDelimiter("|");


        Config config = new Config();
        config.setNumWorkers(3);

        KafkaSpoutConfig spoutConf =  KafkaSpoutConfig.builder("ip-10-28-124-152:6667", "benchmark-3-20")
                .setGroupId("kafka-storm-test")
                .setOffsetCommitPeriodMs(1000)
                .setFirstPollOffsetStrategy(UNCOMMITTED_LATEST)
                .setMaxUncommittedOffsets(1000000)
                .setRecordTranslator(new TupleBuilder(), new Fields("id","uuid"))
                .build();

        HdfsBolt bolt = new HdfsBolt()
                .withFsUrl("hdfs://ip-10-28-124-152:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy)
                .addRotationAction(new MoveFileAction().toDestination("/tmp/dest2/"));

        TopologyBuilder builder = new TopologyBuilder();
//        builder.setSpout("hdfs-spout", new HDFSSpout(), 3);
        builder.setSpout("kafka-spout", new KafkaSpout(spoutConf), 6);
        builder.setBolt("hdfs-bolt", bolt, 4).shuffleGrouping("kafka-spout");

        StormSubmitter.submitTopology("mytopology", config, builder.
                createTopology());

    }
}

class TupleBuilder implements Func<ConsumerRecord<String, String>, List<Object>> {

    public List<Object> apply(ConsumerRecord<String, String> record) {
        Object records[] = record.value().split("\\|");
        return Arrays.asList(records);
    }
}
