package com.jms.zk.curator.example;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.transaction.CuratorTransaction;
import org.apache.curator.framework.api.transaction.CuratorTransactionFinal;
import org.apache.curator.framework.api.transaction.CuratorTransactionResult;
import org.apache.curator.utils.CloseableUtils;

import java.util.Collection;

/**
 * @Author James.xiao
 * @Version Copyright © 2023
 * @Description Curator框架事务处理示例
 * @Date 2023/10/09 16:59
 */
public class TransactionExample {
    public static void main(String[] args) {
        // 1.建立连接
        String connString = "192.169.7.171:2181";
        CuratorFramework client = ConnectionExample.getConn2(connString);
        client.start();

        try {
            // 2.开启事务
            CuratorTransaction transaction = client.inTransaction();

            // 3.事务处理(以下模拟业务处理异常回滚)
            String path = "/jms";
            CuratorTransactionFinal transactionFinal = transaction.create().forPath(path).and()
                    .create().forPath(path + "/test1", "test1".getBytes()).and()
                    .create().forPath(path + "/test2", "test2".getBytes()).and()
                    .setData().forPath(path + "/test2", "test22".getBytes()).and()
                    .create().forPath(path + "/test1", "test1".getBytes()).and();

            // 4.提交事务
            Collection<CuratorTransactionResult> results = transactionFinal.commit();

            // 5.打印结果
            for (CuratorTransactionResult result : results) {
                System.out.println(result.getForPath() + ": " + result.getType());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(client);
        }
    }
}
