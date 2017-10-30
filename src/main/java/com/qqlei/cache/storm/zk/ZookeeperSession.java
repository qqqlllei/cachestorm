package com.qqlei.cache.storm.zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by 李雷 on 2017/10/30.
 */
public class ZookeeperSession {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private ZooKeeper zookeeper;

    public ZookeeperSession(){
        try {
            this.zookeeper = new ZooKeeper(
                    "10.33.80.107:2181,10.33.80.108:2181,10.33.80.109:2181",
                    50000,
                    new Watcher() {
                        public void process(WatchedEvent watchedEvent) {
                            System.out.println("Receive watched event: " + watchedEvent.getState());
                            if(Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                                connectedSemaphore.countDown();
                            }
                        }
                    });

            try {
                this.connectedSemaphore.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("ZooKeeper session established......");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取锁
     */
    public void acquireDistributedLock(){
        String path = "/taskid-list-lock";
        try {
            zookeeper.create(path, "".getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            // 如果那个商品对应的锁的node，已经存在了，就是已经被别人加锁了，那么就这里就会报错
            // NodeExistsException
            int count = 0;
            while(true) {
                try {
                    Thread.sleep(1000);
                    zookeeper.create(path, "".getBytes(),
                            ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (Exception e2) {
                    count++;
                    System.out.println("the " + count + " times try to acquire lock for taskid-list-lock......");
                    continue;
                }
                System.out.println("success to acquire lock for taskid-list-lock after " + count + " times try......");
                break;
            }
        }
    }

    /**
     * 释放锁
     */
    public void releaseDistributedLock(){
        String path = "/taskid-list-lock";
        try {
            this.zookeeper.delete(path,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取节点数据
     * @param path
     * @return
     */
    public String getNode(String path){
        try {
            return new String(zookeeper.getData(path,false,new Stat()));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * 设置节点数据
     * @param path
     * @param data
     */
    public void setNode(String path,String data){
        try {
            if (zookeeper.exists(path, true) != null) {
                zookeeper.setData(path, data.getBytes(), -1);
            }else{
                zookeeper.create(path, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    private static class Singleton{
        private static ZookeeperSession instance;
        static {
            instance = new ZookeeperSession();
        }
        public static ZookeeperSession getInstance(){
            return instance;
        }
    }

    public static ZookeeperSession getInstance(){
        return Singleton.getInstance();
    }
}
