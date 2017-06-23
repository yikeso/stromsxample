package com.example;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by once on 2017/6/23.
 */
public class CountWords {


    public static void main(String[] arg0){
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("sentenceSpout",new SentenceSpout());
        builder.setBolt("splitSentenceBySpace", new SplitSentence(), 1)
                .shuffleGrouping("sentenceSpout");
        builder.setBolt("countWordNum",new WordCount(),1)
                .globalGrouping("splitSentenceBySpace");
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCount",conf,builder.createTopology());
        Utils.sleep(10000);
        cluster.killTopology("wordCount");
        cluster.shutdown();
    }

    static class SentenceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private String[] sentences = {
                "my dog has fleas",
                "i like cold beverages",
                "the dog ate my homework",
                "don't have a cow man",
                "i don't think i like fleas"
        };
        private int index = 0;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void nextTuple() {
            this.collector.emit(new Values(sentences[index]));
            index++;
            if (index >= sentences.length) {
                index = 0;
            }
            Utils.sleep(100);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("sentence"));
        }
    }

    static class SplitSentence implements IBasicBolt{

        @Override
        public void prepare(Map map, TopologyContext topologyContext) {

        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String sentence = tuple.getString(0);
            for (String s:sentence.split("\\s+")){
                basicOutputCollector.emit(new Values(s));
            }
        }

        @Override
        public void cleanup() {

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    static class WordCount implements IBasicBolt {
        Map<String,Long> count = new HashMap<String, Long>();
        @Override
        public void prepare(Map stormConf, TopologyContext context) {

        }

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String word = input.getString(0);
            long n;
            if (count.containsKey(word)){
                n = count.get(word).longValue()+1;
                count.put(word,Long.valueOf(n));
            }else {
                n = 1;
                count.put(word,Long.valueOf(n));
            }
            System.out.println(word+": "+count);
            collector.emit(new Values(word,n));
        }

        @Override
        public void cleanup() {

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
//            declarer.declare(new Fields("word","count"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }
}
