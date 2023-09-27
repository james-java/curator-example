package com.jms.zk.curator.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * @Author James.xiao
 * @Version Copyright © 2023
 * @Description 增删改查示例
 * @Date 2023/09/27 14:57
 */
public class CrudExample {
    public static void main(String[] args) {
        // 1.建立连接
        String connString = "192.169.7.171:2181";
        CuratorFramework client = ConnectionExample.getConn2(connString);
        client.start();

        // 2.创建节点
        create(client, "/test1", "测试1", CreateMode.PERSISTENT); // 持久节点
        create(client, "/test2", "测试2", CreateMode.PERSISTENT_SEQUENTIAL); // 持久顺序节点
        create(client, "/test3", "测试3", CreateMode.EPHEMERAL); // 临时节点
        create(client, "/test4", "测试4", CreateMode.EPHEMERAL_SEQUENTIAL); // 临时顺序节点

        // 3.查询节点
        System.out.println(getData(client, "/test1"));
        System.out.println(getChildren(client, "/"));
        System.out.println(getStatus(client, "/test1"));

        // 4.修改节点
        update(client, "/test1", "测试修改1");
        updateWithVersion(client, "/test1", "测试修改2");

        // 5.删除节点
        deletingGuaranteed(client, "/test1", new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                System.out.println(event);
            }
        });

        // 6.关闭连接
        try {
            Thread.sleep(60000L);
            client.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建节点
     *
     * @param client    客户端对象
     * @param path      路径
     * @param data      数据
     * @param mode      创建模式
     * @throws Exception
     */
    public static void create(CuratorFramework client, String path, String data, CreateMode mode) {
        try {
            client.create().withMode(mode).forPath(path, data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 查询节点数据
     *
     * @param client    客户端对象
     * @param path      路径
     */
    public static String getData(CuratorFramework client, String path) {
        try {
            return new String(client.getData().forPath(path));
        } catch (Exception e) {
            e.printStackTrace();
        }

        return "";
    }

    /**
     * 查询子节点数据
     *
     * @param client    客户端对象
     * @param path      路径
     */
    public static List<String> getChildren(CuratorFramework client, String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 查询节点信息
     *
     * @param client    客户端对象
     * @param path      路径
     */
    public static Stat getStatus(CuratorFramework client, String path) {
        try {
            Stat status = new Stat();
            client.getData().storingStatIn(status).forPath(path);

            return status;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * 更新节点
     *
     * @param client    客户端对象
     * @param path      路径
     * @param data      数据
     */
    public static void update(CuratorFramework client, String path, String data) {
        try {
            client.setData().forPath(path, data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 指定版本号更新节点(如果版本号变了会抛出异常)
     *
     * @param client    客户端对象
     * @param path      路径
     * @param data      数据
     */
    public static void updateWithVersion(CuratorFramework client, String path, String data) {
        try {
            // 查询节点状态信息
            Stat status = new Stat();
            client.getData().storingStatIn(status).forPath(path);
            int version = status.getVersion();

            client.setData().withVersion(version).forPath(path, data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点
     *
     * @param client    客户端对象
     * @param path      路径
     */
    public static void delete(CuratorFramework client, String path) {
        try {
            client.delete().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除带有子节点的节点
     *
     * @param client    客户端对象
     * @param path      路径
     */
    public static void deletingChildrenIfNeeded(CuratorFramework client, String path) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除节点(超时重试删除和带回调功能)
     *
     * @param client    客户端对象
     * @param path      路径
     * @param callback  删除回调
     */
    public static void deletingGuaranteed(CuratorFramework client, String path, BackgroundCallback callback) {
        try {
            client.delete().guaranteed().inBackground(callback).forPath(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
