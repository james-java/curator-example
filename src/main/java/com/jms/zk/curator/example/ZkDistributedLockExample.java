package com.jms.zk.curator.example;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Author James.xiao
 * @Version Copyright © 2023
 * @Description 原生Zookeeper代码实现分布式锁
 * @Date 2023/10/08 10:21
 */
public class ZkDistributedLockExample {
    public static void main(String[] args) throws Exception {
        String connString = "192.169.7.171:2181";
        int sessionTimeOut = 3000;
        String lockPath = "/jms";
        // 创建两个分布式锁，模拟两个进程抢占分布式锁流程
        DistributedLock lock1 = new DistributedLock(connString, sessionTimeOut, lockPath);
        DistributedLock lock2 = new DistributedLock(connString, sessionTimeOut, lockPath);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock1.lock();
                    System.out.println("线程 [" + Thread.currentThread().getName() + "]" + "->抢到分布式锁--开始工作");

                    Thread.sleep(5000);
                    lock1.unlock();
                    System.out.println("线程 [" + Thread.currentThread().getName() + "]" + "->释放分布式锁--结束工作");
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    lock2.lock();
                    System.out.println("线程 [" + Thread.currentThread().getName() + "]" + "->抢到分布式锁--开始工作");

                    Thread.sleep(5000);
                    lock2.unlock();
                    System.out.println("线程 [" + Thread.currentThread().getName() + "]" + "->释放分布式锁--结束工作");
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static class DistributedLock {
        private ZooKeeper client;

        /**
         * 等待zk连接成功
         */
        private CountDownLatch countDownLatch;

        /**
         * 等待节点变化
         */
        private CountDownLatch waitLatch;

        /**
         * 当前节点
         */
        private String currentNode;

        /**
         * 前一个节点路径
         */
        private String waitPath;

        /**
         * 根节点
         */
        private String lockPath;

        public DistributedLock(String connectString, int sessionTimeOut, String path) throws Exception {
            countDownLatch = new CountDownLatch(1);
            waitLatch = new CountDownLatch(1);
            lockPath = path;
            client = new ZooKeeper(connectString, sessionTimeOut, watchedEvent -> {
                // 连上ZK后，释放
                if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    countDownLatch.countDown();
                }

                // waitLatch 需要释放 (节点被删除并且删除的是前一个节点)
                if (watchedEvent.getType() == Watcher.Event.EventType.NodeDeleted &&
                        watchedEvent.getPath().equals(waitPath)) {
                    waitLatch.countDown();
                }
            });

            // 等待Zookeeper连接成功，连接完成继续往下走
            countDownLatch.await();

            Stat stat = client.exists(lockPath, false);
            if (stat == null) {
                // 根节点不存在，则创建
                client.create(lockPath, lockPath.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }

        /**
         * 加锁
         */
        public void lock() {
            try {
                // 创建有序临时子节点
                currentNode = client.create(lockPath + "/seq-", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

                // 如果是最小序号节点，则获取锁；如果不是就监听前一个节点
                List<String> children = client.getChildren(lockPath, false);

                // 子节点排序
                Collections.sort(children);

                // 截取子节点名称
                String nodeName = currentNode.substring((lockPath + "/").length());

                // 通过名称获取子节点在集合的位置
                int index = children.indexOf(nodeName);

                if (index == -1) {
                    System.out.println("数据异常");
                } else if (index == 0) {
                    // 最小序号子节点，则获取锁
                    return;
                } else {
                    // 监听前一个节点变化
                    waitPath = (lockPath + "/") + children.get(index-1);
                    client.getData(waitPath,true,null);

                    waitLatch.await();
                    return;
                }

            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /**
         * 释放锁
         *
         * @throws KeeperException
         * @throws InterruptedException
         */
        public void unlock() throws KeeperException, InterruptedException {
            client.delete(currentNode, -1);
        }
    }
}
