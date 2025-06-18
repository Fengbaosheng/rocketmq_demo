package org.example;

import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

public class OrderProducer {

    /**
     * The number of produced messages.
     */
    public static final String PRODUCER_GROUP = "OrderGroup";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "OrderTopic";

    public static void main(String[] args) throws MQClientException {
        try {
            // 创建 ACL 认证钩子（注入 AccessKey 和 SecretKey）
            AclClientRPCHook rpcHook = new AclClientRPCHook(
                    new SessionCredentials("rocketmq2", "rocketmq2@2025"));

            /*
             * Instantiate with a producer group name.
             */
            DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP, rpcHook);
            producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);

            producer.start();

            //假设有六个业务，每个业务需要经历三个步骤
            for (int i = 0; i < 6; i++) {
                int orderId = i;
                for (int j = 0; j < 7; j++) {
                    Message msg =
                            new Message(TOPIC, "order_" + orderId, "key_" + orderId,
                                    ("order_" + orderId + " step " + j).getBytes(RemotingHelper.DEFAULT_CHARSET));
                    SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                            Integer id = (Integer) arg;
                            int index = id % mqs.size();
                            return mqs.get(index);
                        }
                    }, orderId);
                    System.out.printf("%s%n", sendResult);
                }
            }

            producer.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            throw new MQClientException(e.getMessage(), null);
        }
    }


}
