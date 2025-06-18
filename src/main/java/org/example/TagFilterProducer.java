package org.example;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class TagFilterProducer {
    /**
     * The number of produced messages.
     */
    public static final int MESSAGE_COUNT = 1;
    public static final String PRODUCER_GROUP = "FilterGroup";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "FilterTopic";

    public static void main(String[] args) throws Exception {
        // 创建 ACL 认证钩子（注入 AccessKey 和 SecretKey）
        AclClientRPCHook rpcHook = new AclClientRPCHook(
                new SessionCredentials("rocketmq2", "rocketmq2@2025"));

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP,rpcHook);
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

        producer.start();

        String[] tags = new String[] {"TagA", "TagB", "TagC"};

        for (int i = 0; i < 5; i++) {
            Message msg = new Message(TOPIC,
                    tags[i % tags.length],
                    "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));

            SendResult sendResult = producer.send(msg);
            System.out.printf("%s%n", sendResult);
        }

        producer.shutdown();
    }
}
