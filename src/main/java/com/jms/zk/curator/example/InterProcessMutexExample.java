package com.jms.zk.curator.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Author James.xiao
 * @Version Copyright © 2023
 * @Description Curator框架实现分布式锁
 * @Date 2023/10/07 16:51
 */
public class InterProcessMutexExample {

    public static void main(String[] args) {
        // 模拟50个进程抢占分布式锁
        String connString = "192.169.7.171:2181";
        int threadCount = 50;
        CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        countDownLatch.await();

                        // 1.创建分布式锁
                        DistributedLock lock = new DistributedLock(connString, "/jms");

                        // 2.抢分布式锁
                        lock.acquire();

                        System.out.println("线程 [" + Thread.currentThread().getName() + "]" + "->抢到分布式锁--开始工作");

                        // 3.模拟执行业务逻辑
                        Thread.sleep(500L);

                        // 4.释放分布式锁
                        lock.release();
                        System.out.println("线程 [" + Thread.currentThread().getName() + "]" + "->释放分布式锁--结束工作");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();

            countDownLatch.countDown();
        }
    }

    public static class DistributedLock {
        private CuratorFramework client;
        private InterProcessMutex mutex;

        public DistributedLock(String connString, String lockPath) {
            this(connString, lockPath, new ExponentialBackoffRetry(3000,5));
        }

        public DistributedLock(String connString, String lockPath, ExponentialBackoffRetry retryPolicy) {
            try {
                client = CuratorFrameworkFactory.builder()
                        .connectString(connString)
                        .retryPolicy(retryPolicy)
                        .build();
                client.start();

                mutex = new InterProcessMutex(client, lockPath);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /**
         * 获取分布式锁
         *
         * @throws Exception
         */
        public void acquire() throws Exception {
            mutex.acquire();
        }

        /**
         * 获取分布式锁(指定时间)
         *
         * @param time  时间
         * @param unit  时间单位
         * @return boolean
         * @throws Exception
         */
        public boolean acquire(long time, TimeUnit unit) throws Exception {
            return mutex.acquire(time, unit);
        }

        /**
         * 释放分布式锁
         *
         * @throws Exception
         */
        public void release() throws Exception {
            mutex.release();
        }
    }
}
