package com.qqlei.cache.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qqlei.cache.storm.zk.ZookeeperSession;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by 李雷 on 2017/10/27.
 */
public class ProductCountBolt extends BaseRichBolt {
    private LRUMap<Long, Long> productCountMap = new LRUMap<Long, Long>(1000);
    private ZookeeperSession zookeeperSession;
    private int taskId ;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.zookeeperSession = ZookeeperSession.getInstance();
        this.taskId = topologyContext.getThisTaskId();
        new Thread(new ProductCountThread()).start();
        initTaskId();
    }


    private void initTaskId(){


        zookeeperSession.acquireDistributedLock();

        String taskIdList = zookeeperSession.getNode("/taskid-list");

        if(!"".equals(taskIdList)) {
            taskIdList += "," + taskId;
        } else {
            taskIdList += taskId;
        }

        zookeeperSession.setNode("/taskid-list",taskIdList);

        zookeeperSession.releaseDistributedLock();
    }

    private class ProductCountThread implements Runnable{

        public void run() {
            List<Map.Entry<Long, Long>> topnProductList = new ArrayList<Map.Entry<Long, Long>>();
            while (true){
                topnProductList.clear();
                int topn = 3;

                if(productCountMap.size() == 0) {
                    Utils.sleep(100);
                    continue;
                }


                for(Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
                    if(topnProductList.size() == 0) {
                        topnProductList.add(productCountEntry);
                    } else {
                        boolean bigger = false;
                        for(int i = 0; i < topnProductList.size(); i++){
                            Map.Entry<Long, Long> topnProductCountEntry = topnProductList.get(i);

                            if(productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                int lastIndex = topnProductList.size() < topn ? topnProductList.size() - 1 : topn - 2;
                                for(int j = lastIndex; j >= i; j--) {
                                    topnProductList.set(j + 1, topnProductList.get(j));
                                }
                                topnProductList.set(i, productCountEntry);
                                bigger = true;
                                break;
                            }
                        }

                        if(!bigger) {
                            if(topnProductList.size() < topn) {
                                topnProductList.add(productCountEntry);
                            }
                        }
                    }
                }

                // 获取到一个topn list
                String topnProductListJSON = JSONArray.toJSONString(topnProductList);
                zookeeperSession.setNode("/task-hot-product-list-" + taskId, topnProductListJSON);
                JSONObject jsonObject = (JSONObject) JSONObject.toJSON(productCountMap);
                zookeeperSession.setNode("/task-lru-product-list",jsonObject.toJSONString());
                Utils.sleep(5000);
            }
        }
    }

    public void execute(Tuple tuple) {
        Long productId = tuple.getLongByField("productId");
        Long count = productCountMap.get(productId);
        if(count == null) {
            count = 0L;
        }
        count++;
        productCountMap.put(productId, count);
        System.out.println("商品productId："+productId+"的访问次数为："+count);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
