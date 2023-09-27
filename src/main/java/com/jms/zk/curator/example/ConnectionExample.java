package com.jms.zk.curator.example;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @Author James.xiao
 * @Version Copyright © 2023
 * @Description zk连接示例
 * @Date 2023/09/27 14:45
 */
public class ConnectionExample {

    public static void main(String[] args) {
        String connString = "192.169.7.171:2181";
        CuratorFramework client;
//        client = getConn1(connString);
        client = getConn2(connString);

        try {
            // 开启连接, 5秒后再关闭连接
            client.start();
            Thread.sleep(5000L);
            client.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 建立zk连接
     *
     * @param connString zk连接字符串, 如"192.169.7.171:2181,192.169.7.172:2181,192.169.7.173:2181"
     * @return CuratorFramework
     */
    public static CuratorFramework getConn1(String connString) {
        //重试策略 baseSleepTimeMs 重试之间等待的初始时间，maxRetries 重试的最大次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000,5);
        return CuratorFrameworkFactory.newClient(connString, 60 * 1000, 15 * 1000, retryPolicy);
    }

    /**
     * 建立zk连接 (Fluent风格)
     *
     * @param connString zk连接字符串, 如"192.169.7.171:2181,192.169.7.172:2181,192.169.7.173:2181"
     * @return CuratorFramework
     */
    public static CuratorFramework getConn2(String connString) {
        //重试策略 baseSleepTimeMs 重试之间等待的初始时间，maxRetries 重试的最大次数
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000,5);
        return CuratorFrameworkFactory.builder()
                .connectString(connString)
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
//                .namespace("jms")  // 根节点名称设置
                .build();
    }
}
