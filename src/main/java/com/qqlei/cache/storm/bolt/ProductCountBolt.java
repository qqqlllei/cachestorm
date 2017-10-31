package com.qqlei.cache.storm.bolt;

import com.alibaba.fastjson.JSONArray;
import com.qqlei.cache.storm.http.HttpClientUtils;
import com.qqlei.cache.storm.zk.ZookeeperSession;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.trident.util.LRUMap;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by 李雷 on 2017/10/27.
 */
public class ProductCountBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProductCountBolt.class);
    private LRUMap<Long, Long> productCountMap = new LRUMap<Long, Long>(1000);
    private ZookeeperSession zookeeperSession;
    private int taskId ;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.zookeeperSession = ZookeeperSession.getInstance();
        this.taskId = topologyContext.getThisTaskId();
        new Thread(new ProductCountThread()).start();
        new Thread(new HotProductFindThread()).start();

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

    private class HotProductFindThread implements Runnable{

        public void run() {
            List<Map.Entry<Long, Long>> productCountList = new ArrayList<Map.Entry<Long, Long>>();
            List<Long> hotProductIdList = new ArrayList<Long>();
            List<Long> lastTimeHotProductIdList = new ArrayList<Long>();
            while (true){
                productCountList.clear();
                hotProductIdList.clear();

                if(productCountMap.size() == 0) {
                    Utils.sleep(100);
                    continue;
                }
                LOGGER.info("【HotProductFindThread打印productCountMap的长度】size=" + productCountMap.size());


                for (Map.Entry<Long, Long> productCountEntry : productCountMap.entrySet()) {
                    if(productCountList.size() == 0){
                        productCountList.add(productCountEntry);
                    }else{

                        boolean bigger = false;
                        for(int i = 0; i < productCountList.size(); i++){
                            Map.Entry<Long, Long> topnProductCountEntry = productCountList.get(i);
                            if(productCountEntry.getValue() > topnProductCountEntry.getValue()) {
                                int lastIndex = productCountList.size() < productCountMap.size() ? productCountList.size() - 1 : productCountMap.size() - 2;
                                for(int j = lastIndex; j >= i; j--) {
                                    if(j + 1 == productCountList.size()) {
                                        productCountList.add(null);
                                    }
                                    productCountList.set(j + 1, productCountList.get(j));
                                }
                                productCountList.set(i, productCountEntry);
                                bigger = true;
                                break;
                            }
                        }

                        if(!bigger) {
                            if(productCountList.size() < productCountMap.size()) {
                                productCountList.add(productCountEntry);
                            }
                        }

                    }


                }

                int calculateCount = (int)Math.floor(productCountList.size() * 0.95);

                Long totalCount = 0L;
                for(int i = productCountList.size() - 1; i >= productCountList.size() - calculateCount; i--) {
                    totalCount += productCountList.get(i).getValue();
                }

                Long avgCount = totalCount / calculateCount;

                for(Map.Entry<Long, Long> productCountEntry : productCountList) {
                    if(productCountEntry.getValue() > 10 * avgCount) {
                        hotProductIdList.add(productCountEntry.getKey());

                        System.out.println("=======================发送热点数据开始："+productCountEntry.getKey()+"=======================");
                        // 将缓存热点反向推送到流量分发的nginx中
                        String distributeNginxURL = "http://10.33.80.106/hot?productId=" + productCountEntry.getKey();
                        HttpClientUtils.sendGetRequest(distributeNginxURL);

                        // 将缓存热点，那个商品对应的完整的缓存数据，发送请求到缓存服务去获取，反向推送到所有的后端应用nginx服务器上去
                        String cacheServiceURL = "http://10.33.50.40:8080/getProductInfo?productId=" + productCountEntry.getKey();
                        String response = HttpClientUtils.sendGetRequest(cacheServiceURL);

                        String[] appNginxURLs = new String[]{
                                "http://10.33.80.104/hot?productId=" + productCountEntry.getKey() + "&productInfo=" + response,
                                "http://10.33.80.105/hot?productId=" + productCountEntry.getKey() + "&productInfo=" + response
                        };

                        for(String appNginxURL : appNginxURLs) {
                            HttpClientUtils.sendGetRequest(appNginxURL);
                        }

                        System.out.println("=======================发送热点数据结束："+productCountEntry.getKey()+"=======================");
                    }
                }

                if(lastTimeHotProductIdList.size() == 0) {
                    if(hotProductIdList.size() > 0) {
                        for(Long productId : hotProductIdList) {
                            lastTimeHotProductIdList.add(productId);
                        }
                        LOGGER.info("【HotProductFindThread保存上次热点数据】lastTimeHotProductIdList=" + lastTimeHotProductIdList);
                    }
                }else {
                    for(Long productId : lastTimeHotProductIdList) {
                        if(!hotProductIdList.contains(productId)) {
                            LOGGER.info("【HotProductFindThread发现一个热点消失了】productId=" + productId);
                            // 说明上次的那个商品id的热点，消失了
                            // 发送一个http请求给到流量分发的nginx中，取消热点缓存的标识
                            String url = "http://10.33.80.106/cancel_hot?productId=" + productId;
                            HttpClientUtils.sendGetRequest(url);
                        }
                    }

                    if(hotProductIdList.size() > 0) {
                        lastTimeHotProductIdList.clear();
                        for(Long productId : hotProductIdList) {
                            lastTimeHotProductIdList.add(productId);
                        }
                        LOGGER.info("【HotProductFindThread保存上次热点数据】lastTimeHotProductIdList=" + lastTimeHotProductIdList);
                    } else {
                        lastTimeHotProductIdList.clear();
                    }
                }

                Utils.sleep(5000);
            }

        }
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

                System.out.println("zookeeperSession.setNode：task-hot-product-list-"+taskId+"数据："+topnProductListJSON);
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
