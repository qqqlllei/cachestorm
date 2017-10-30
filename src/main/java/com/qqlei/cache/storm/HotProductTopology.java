package com.qqlei.cache.storm;

import com.qqlei.cache.storm.bolt.LogParseBolt;
import com.qqlei.cache.storm.bolt.ProductCountBolt;
import com.qqlei.cache.storm.spout.AccessLogKafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Created by 李雷 on 2017/10/27.
 */
public class HotProductTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("AccessLogKafkaSpout", new AccessLogKafkaSpout(), 1);
        builder.setBolt("LogParseBolt",new LogParseBolt(),5).setNumTasks(5).shuffleGrouping("AccessLogKafkaSpout");
        builder.setBolt("ProductCountBolt",new ProductCountBolt(),5).fieldsGrouping("LogParseBolt",new Fields("productId"));

        Config config = new Config();

        if(args != null && args.length > 0) {
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], config, builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("HotProductTopology", config, builder.createTopology());
            Utils.sleep(3000000);
            cluster.shutdown();
        }
    }

}
