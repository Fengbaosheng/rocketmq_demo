package org.example;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;


import java.nio.charset.StandardCharsets;

public class ScheduledMessageProducer {

    /**
     * The number of produced messages.
     */
    public static final String PRODUCER_GROUP = "ScheduledGroup";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "ScheduledTopic";

    public static void main(String[] args) throws MQClientException {
        // 创建 ACL 认证钩子（注入 AccessKey 和 SecretKey）
        AclClientRPCHook rpcHook = new AclClientRPCHook(
                new SessionCredentials("rocketmq2", "rocketmq2@2025"));

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP, rpcHook);

        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);
        producer.start();
        try {
            Message message = new Message(TOPIC, ("this is delay message").getBytes(StandardCharsets.UTF_8));
            message.setDelayTimeLevel(3);
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        } catch (Exception e) {
            e.printStackTrace();
            throw new MQClientException(e.getMessage(), null);
        } finally {
            producer.shutdown();
        }
    }
}
