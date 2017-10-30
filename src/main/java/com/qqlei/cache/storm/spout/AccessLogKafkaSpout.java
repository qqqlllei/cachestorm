package com.qqlei.cache.storm.spout;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by 李雷 on 2017/10/27.
 */
public class AccessLogKafkaSpout extends BaseRichSpout{
    private static final long serialVersionUID = 8698470299234327074L;
    private ArrayBlockingQueue<String> queue = new ArrayBlockingQueue(1000);

    private SpoutOutputCollector collector;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector= spoutOutputCollector;
        startKafkaConsumer();
    }

    public void nextTuple() {
        if(queue.size() > 0) {
            try {
                String message = queue.take();
                collector.emit(new Values(message));
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            Utils.sleep(100);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("message"));
    }

    private void startKafkaConsumer() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "10.33.80.107:2181,10.33.80.108:2181,10.33.80.109:2181");
        props.put("group.id", "qqlei-storm-group");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);

        ConsumerConnector consumerConnector = Consumer.
                createJavaConsumerConnector(consumerConfig);
        String topic = "access-log";

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        for (KafkaStream stream : streams) {
            new Thread(new KafkaMessageProcessor(stream)).start();
        }
    }

    private class KafkaMessageProcessor implements Runnable {


        private KafkaStream kafkaStream;


        public KafkaMessageProcessor(KafkaStream kafkaStream) {
            this.kafkaStream = kafkaStream;
        }


        public void run() {
            ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
            while (it.hasNext()) {
                String message = new String(it.next().message());
                try {
                    queue.put(message);
                } catch (InterruptedException e) {
                    System.out.println("=========================KafkaMessageProcessor run 被打断=========================");
                    e.printStackTrace();
                }
            }
        }

    }
}
