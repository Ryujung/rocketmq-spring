/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.spring.support;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;

import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.utils.MessageUtil;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.annotation.SelectorType;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQPushConsumerLifecycleListener;
import org.apache.rocketmq.spring.core.RocketMQReplyListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.SmartMessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;
import org.springframework.util.MimeTypeUtils;

/**
 * 用于管理 RocketMQ 消息监听器的注册、启动、停止和处理的监听器容器。
 *
 * 是一个监听器容器,它管理了一个或多个MessageListener实例。这个Container会负责MessageListener的生命周期管理,包括:
 * - 启动所有MessageListener
 * - 停止所有MessageListener
 * - 重新启动MessageListener 当其突然异常退出时
 *
 * Container会保证其内部管理的所有MessageListener均处于运行状态,
 * 这为上层应用提供了便利,无需自行管理MessageListener的生命周期。
 *
 * 采用了Facade模式,它隐藏了内部多个MessageListener的细节,对外提供统一的管理接口。
 */
@SuppressWarnings("WeakerAccess")
public class DefaultRocketMQListenerContainer implements InitializingBean,
        RocketMQListenerContainer, SmartLifecycle, ApplicationContextAware {
    private final static Logger log = LoggerFactory.getLogger(DefaultRocketMQListenerContainer.class);

    private ApplicationContext applicationContext;

    /**
     * The name of the DefaultRocketMQListenerContainer instance
     */
    private String name;

    /**
     * Suspending pulling time in orderly mode.
     *
     * The minimum value is 10 and the maximum is 30000.
     */
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * Message consume retry strategy in concurrently mode.
     *
     * -1,no retry,put into DLQ directly
     * 0,broker control retry frequency
     * >0,client control retry frequency
     */
    private int delayLevelWhenNextConsume = 0;

    private String nameServer;

    private AccessChannel accessChannel = AccessChannel.LOCAL;

    private String consumerGroup;

    private String topic;

    private int consumeThreadMax = 64;

    private int consumeThreadNumber = 20;

    private String charset = "UTF-8";

    private MessageConverter messageConverter;

    private RocketMQListener rocketMQListener;

    private RocketMQReplyListener rocketMQReplyListener;

    private RocketMQMessageListener rocketMQMessageListener;

    private DefaultMQPushConsumer consumer;

    private Type messageType;

    private MethodParameter methodParameter;

    private boolean running;

    // The following properties came from @RocketMQMessageListener.
    private ConsumeMode consumeMode;
    private SelectorType selectorType;
    private String selectorExpression;
    private MessageModel messageModel;
    private long consumeTimeout;
    private int maxReconsumeTimes;
    private int replyTimeout;
    private String tlsEnable;
    private String namespace;
    private long awaitTerminationMillisWhenShutdown;

    private String instanceName;

    public long getSuspendCurrentQueueTimeMillis() {
        return suspendCurrentQueueTimeMillis;
    }

    public void setSuspendCurrentQueueTimeMillis(long suspendCurrentQueueTimeMillis) {
        this.suspendCurrentQueueTimeMillis = suspendCurrentQueueTimeMillis;
    }

    public int getDelayLevelWhenNextConsume() {
        return delayLevelWhenNextConsume;
    }

    public void setDelayLevelWhenNextConsume(int delayLevelWhenNextConsume) {
        this.delayLevelWhenNextConsume = delayLevelWhenNextConsume;
    }

    public String getNameServer() {
        return nameServer;
    }

    public void setNameServer(String nameServer) {
        this.nameServer = nameServer;
    }

    public AccessChannel getAccessChannel() {
        return accessChannel;
    }

    public void setAccessChannel(AccessChannel accessChannel) {
        this.accessChannel = accessChannel;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getConsumeThreadMax() {
        return consumeThreadMax;
    }

    public int getConsumeThreadNumber() {
        return consumeThreadNumber;
    }

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
        this.charset = charset;
    }

    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

    public DefaultRocketMQListenerContainer setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
        return this;
    }

    public RocketMQListener getRocketMQListener() {
        return rocketMQListener;
    }

    public void setRocketMQListener(RocketMQListener rocketMQListener) {
        this.rocketMQListener = rocketMQListener;
    }

    public RocketMQReplyListener getRocketMQReplyListener() {
        return rocketMQReplyListener;
    }

    public void setRocketMQReplyListener(RocketMQReplyListener rocketMQReplyListener) {
        this.rocketMQReplyListener = rocketMQReplyListener;
    }

    public RocketMQMessageListener getRocketMQMessageListener() {
        return rocketMQMessageListener;
    }

    public void setRocketMQMessageListener(RocketMQMessageListener anno) {
        this.rocketMQMessageListener = anno;

        this.consumeMode = anno.consumeMode();
        this.consumeThreadMax = anno.consumeThreadNumber();
        this.consumeThreadNumber = anno.consumeThreadNumber();
        this.messageModel = anno.messageModel();
        this.selectorType = anno.selectorType();
        this.selectorExpression = anno.selectorExpression();
        this.consumeTimeout = anno.consumeTimeout();
        this.maxReconsumeTimes = anno.maxReconsumeTimes();
        this.replyTimeout = anno.replyTimeout();
        this.tlsEnable = anno.tlsEnable();
        this.namespace = anno.namespace();
        this.delayLevelWhenNextConsume = anno.delayLevelWhenNextConsume();
        this.suspendCurrentQueueTimeMillis = anno.suspendCurrentQueueTimeMillis();
        this.awaitTerminationMillisWhenShutdown = Math.max(0, anno.awaitTerminationMillisWhenShutdown());
        this.instanceName = anno.instanceName();
    }

    public ConsumeMode getConsumeMode() {
        return consumeMode;
    }

    public SelectorType getSelectorType() {
        return selectorType;
    }

    public void setSelectorExpression(String selectorExpression) {
        this.selectorExpression = selectorExpression;
    }

    public String getSelectorExpression() {
        return selectorExpression;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public String getTlsEnable() {
        return tlsEnable;
    }

    public void setTlsEnable(String tlsEnable) {
        this.tlsEnable = tlsEnable;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    public void setConsumer(DefaultMQPushConsumer consumer) {
        this.consumer = consumer;
    }

    public long getAwaitTerminationMillisWhenShutdown() {
        return awaitTerminationMillisWhenShutdown;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public DefaultRocketMQListenerContainer setAwaitTerminationMillisWhenShutdown(long awaitTerminationMillisWhenShutdown) {
        this.awaitTerminationMillisWhenShutdown = awaitTerminationMillisWhenShutdown;
        return this;
    }

    @Override
    public void destroy() {
        this.setRunning(false);
        if (Objects.nonNull(consumer)) {
            consumer.shutdown();
        }
        log.info("container destroyed, {}", this.toString());
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    /**
     * spring组件生命周期接口方法，用于启动当前组件
     */
    @Override
    public void start() {
        if (this.isRunning()) {
            throw new IllegalStateException("container already running. " + this.toString());
        }

        try {
            consumer.start();
        } catch (MQClientException e) {
            throw new IllegalStateException("Failed to start RocketMQ push consumer", e);
        }
        this.setRunning(true);

        log.info("running container: {}", this.toString());
    }

    @Override
    public void stop() {
        if (this.isRunning()) {
            if (Objects.nonNull(consumer)) {
                consumer.shutdown();
            }
            setRunning(false);
        }
    }

    /**
     * 返回一个布尔值，指示组件当前是否正在运行中。
     */
    @Override
    public boolean isRunning() {
        return running;
    }

    private void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * 返回一个整数值，表示组件的启动和停止顺序。
     * 在容器启动过程中，按照 getPhase() 方法的返回值从小到大的顺序启动组件，容器关闭过程中则按相反的顺序停止组件。
     */
    @Override
    public int getPhase() {
        // Returning Integer.MAX_VALUE only suggests that
        // we will be the first bean to shutdown and last bean to start
        return Integer.MAX_VALUE;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initRocketMQPushConsumer();

        this.messageType = getMessageType();
        this.methodParameter = getMethodParameter();
        log.debug("RocketMQ messageType: {}", messageType);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public String toString() {
        return "DefaultRocketMQListenerContainer{" +
                "consumerGroup='" + consumerGroup + '\'' +
                ", namespace='" + namespace + '\'' +
                ", nameServer='" + nameServer + '\'' +
                ", topic='" + topic + '\'' +
                ", consumeMode=" + consumeMode +
                ", selectorType=" + selectorType +
                ", selectorExpression='" + selectorExpression + '\'' +
                ", messageModel=" + messageModel + '\'' +
                ", tlsEnable=" + tlsEnable +
                ", instanceName=" + instanceName +
                '}';
    }

    public void setName(String name) {
        this.name = name;
    }

    public class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for (MessageExt messageExt : msgs) {
                log.debug("received msg: {}", messageExt);
                try {
                    long now = System.currentTimeMillis();
                    handleMessage(messageExt);
                    long costTime = System.currentTimeMillis() - now;
                    log.debug("consume {} cost: {} ms", messageExt.getMsgId(), costTime);
                } catch (Exception e) {
                    log.warn("consume message failed. messageId:{}, topic:{}, reconsumeTimes:{}", messageExt.getMsgId(), messageExt.getTopic(), messageExt.getReconsumeTimes(), e);
                    context.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    public class DefaultMessageListenerOrderly implements MessageListenerOrderly {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            for (MessageExt messageExt : msgs) {
                log.debug("received msg: {}", messageExt);
                try {
                    long now = System.currentTimeMillis();
                    handleMessage(messageExt);
                    long costTime = System.currentTimeMillis() - now;
                    log.debug("consume {} cost: {} ms", messageExt.getMsgId(), costTime);
                } catch (Exception e) {
                    log.warn("consume message failed. messageId:{}, topic:{}, reconsumeTimes:{}", messageExt.getMsgId(), messageExt.getTopic(), messageExt.getReconsumeTimes(), e);
                    context.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                }
            }

            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

    private void handleMessage(
            MessageExt messageExt) throws MQClientException, RemotingException, InterruptedException {
        if (rocketMQListener != null) {
            rocketMQListener.onMessage(doConvertMessage(messageExt));
        } else if (rocketMQReplyListener != null) {
            Object replyContent = rocketMQReplyListener.onMessage(doConvertMessage(messageExt));
            Message<?> message = MessageBuilder.withPayload(replyContent).build();

            org.apache.rocketmq.common.message.Message replyMessage = MessageUtil.createReplyMessage(messageExt, convertToBytes(message));
            DefaultMQProducer producer = consumer.getDefaultMQPushConsumerImpl().getmQClientFactory().getDefaultMQProducer();
            producer.setSendMsgTimeout(replyTimeout);
            producer.send(replyMessage, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                        log.error("Consumer replies message failed. SendStatus: {}", sendResult.getSendStatus());
                    } else {
                        log.debug("Consumer replies message success.");
                    }
                }

                @Override
                public void onException(Throwable e) {
                    log.error("Consumer replies message failed. error: {}", e.getLocalizedMessage());
                }
            });
        }
    }

    private byte[] convertToBytes(Message<?> message) {
        Message<?> messageWithSerializedPayload = doConvert(message.getPayload(), message.getHeaders());
        Object payloadObj = messageWithSerializedPayload.getPayload();
        byte[] payloads;
        try {
            if (null == payloadObj) {
                throw new RuntimeException("the message cannot be empty");
            }
            if (payloadObj instanceof String) {
                payloads = ((String) payloadObj).getBytes(Charset.forName(charset));
            } else if (payloadObj instanceof byte[]) {
                payloads = (byte[]) messageWithSerializedPayload.getPayload();
            } else {
                String jsonObj = (String) this.messageConverter.fromMessage(messageWithSerializedPayload, payloadObj.getClass());
                if (null == jsonObj) {
                    throw new RuntimeException(String.format(
                            "empty after conversion [messageConverter:%s,payloadClass:%s,payloadObj:%s]",
                            this.messageConverter.getClass(), payloadObj.getClass(), payloadObj));
                }
                payloads = jsonObj.getBytes(Charset.forName(charset));
            }
        } catch (Exception e) {
            throw new RuntimeException("convert to bytes failed.", e);
        }
        return payloads;
    }

    private Message<?> doConvert(Object payload, MessageHeaders headers) {
        Message<?> message = this.messageConverter instanceof SmartMessageConverter ?
                ((SmartMessageConverter) this.messageConverter).toMessage(payload, headers, null) :
                this.messageConverter.toMessage(payload, headers);
        if (message == null) {
            String payloadType = payload.getClass().getName();
            Object contentType = headers != null ? headers.get(MessageHeaders.CONTENT_TYPE) : null;
            throw new MessageConversionException("Unable to convert payload with type='" + payloadType +
                    "', contentType='" + contentType + "', converter=[" + this.messageConverter + "]");
        }
        MessageBuilder<?> builder = MessageBuilder.fromMessage(message);
        builder.setHeaderIfAbsent(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN);
        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private Object doConvertMessage(MessageExt messageExt) {
        if (Objects.equals(messageType, MessageExt.class) || Objects.equals(messageType, org.apache.rocketmq.common.message.Message.class)) {
            return messageExt;
        } else {
            String str = new String(messageExt.getBody(), Charset.forName(charset));
            if (Objects.equals(messageType, String.class)) {
                return str;
            } else {
                // If msgType not string, use objectMapper change it.
                try {
                    if (messageType instanceof Class) {
                        //if the messageType has not Generic Parameter
                        return this.getMessageConverter().fromMessage(MessageBuilder.withPayload(str).build(), (Class<?>) messageType);
                    } else {
                        //if the messageType has Generic Parameter, then use SmartMessageConverter#fromMessage with third parameter "conversionHint".
                        //we have validate the MessageConverter is SmartMessageConverter in this#getMethodParameter.
                        return ((SmartMessageConverter) this.getMessageConverter()).fromMessage(MessageBuilder.withPayload(str).build(), (Class<?>) ((ParameterizedType) messageType).getRawType(), methodParameter);
                    }
                } catch (Exception e) {
                    log.info("convert failed. str:{}, msgType:{}", str, messageType);
                    throw new RuntimeException("cannot convert message to " + messageType, e);
                }
            }
        }
    }

    private MethodParameter getMethodParameter() {
        Class<?> targetClass;
        if (rocketMQListener != null) {
            targetClass = AopProxyUtils.ultimateTargetClass(rocketMQListener);
        } else {
            targetClass = AopProxyUtils.ultimateTargetClass(rocketMQReplyListener);
        }
        Type messageType = this.getMessageType();
        Class clazz = null;
        if (messageType instanceof ParameterizedType && messageConverter instanceof SmartMessageConverter) {
            clazz = (Class) ((ParameterizedType) messageType).getRawType();
        } else if (messageType instanceof Class) {
            clazz = (Class) messageType;
        } else {
            throw new RuntimeException("parameterType:" + messageType + " of onMessage method is not supported");
        }
        try {
            final Method method = targetClass.getMethod("onMessage", clazz);
            return new MethodParameter(method, 0);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
            throw new RuntimeException("parameterType:" + messageType + " of onMessage method is not supported");
        }
    }

    private Type getMessageType() {
        Class<?> targetClass;
        if (rocketMQListener != null) {
            targetClass = AopProxyUtils.ultimateTargetClass(rocketMQListener);
        } else {
            targetClass = AopProxyUtils.ultimateTargetClass(rocketMQReplyListener);
        }
        Type matchedGenericInterface = null;
        while (Objects.nonNull(targetClass)) {
            Type[] interfaces = targetClass.getGenericInterfaces();
            if (Objects.nonNull(interfaces)) {
                for (Type type : interfaces) {
                    if (type instanceof ParameterizedType &&
                            (Objects.equals(((ParameterizedType) type).getRawType(), RocketMQListener.class) || Objects.equals(((ParameterizedType) type).getRawType(), RocketMQReplyListener.class))) {
                        matchedGenericInterface = type;
                        break;
                    }
                }
            }
            targetClass = targetClass.getSuperclass();
        }
        if (Objects.isNull(matchedGenericInterface)) {
            return Object.class;
        }

        Type[] actualTypeArguments = ((ParameterizedType) matchedGenericInterface).getActualTypeArguments();
        if (Objects.nonNull(actualTypeArguments) && actualTypeArguments.length > 0) {
            return actualTypeArguments[0];
        }
        return Object.class;
    }

    private void initRocketMQPushConsumer() throws MQClientException {
        if (rocketMQListener == null && rocketMQReplyListener == null) {
            throw new IllegalArgumentException("Property 'rocketMQListener' or 'rocketMQReplyListener' is required");
        }
        Assert.notNull(consumerGroup, "Property 'consumerGroup' is required");
        Assert.notNull(nameServer, "Property 'nameServer' is required");
        Assert.notNull(topic, "Property 'topic' is required");

        RPCHook rpcHook = RocketMQUtil.getRPCHookByAkSk(applicationContext.getEnvironment(),
                this.rocketMQMessageListener.accessKey(), this.rocketMQMessageListener.secretKey());
        boolean enableMsgTrace = rocketMQMessageListener.enableMsgTrace();
        if (Objects.nonNull(rpcHook)) {
            consumer = new DefaultMQPushConsumer(consumerGroup, rpcHook, new AllocateMessageQueueAveragely(),
                    enableMsgTrace, this.applicationContext.getEnvironment().
                    resolveRequiredPlaceholders(this.rocketMQMessageListener.customizedTraceTopic()));
            consumer.setVipChannelEnabled(false);
        } else {
            log.debug("Access-key or secret-key not configure in " + this + ".");
            consumer = new DefaultMQPushConsumer(consumerGroup, enableMsgTrace,
                    this.applicationContext.getEnvironment().
                            resolveRequiredPlaceholders(this.rocketMQMessageListener.customizedTraceTopic()));
        }
        consumer.setNamespace(namespace);

        String customizedNameServer = this.applicationContext.getEnvironment().resolveRequiredPlaceholders(this.rocketMQMessageListener.nameServer());
        if (customizedNameServer != null) {
            consumer.setNamesrvAddr(customizedNameServer);
        } else {
            consumer.setNamesrvAddr(nameServer);
        }
        if (accessChannel != null) {
            consumer.setAccessChannel(accessChannel);
        }
        //set the consumer core thread number and maximum thread number has the same value
        consumer.setConsumeThreadMax(consumeThreadNumber);
        consumer.setConsumeThreadMin(consumeThreadNumber);
        consumer.setConsumeTimeout(consumeTimeout);
        consumer.setMaxReconsumeTimes(maxReconsumeTimes);
        consumer.setAwaitTerminationMillisWhenShutdown(awaitTerminationMillisWhenShutdown);
        consumer.setInstanceName(instanceName);
        switch (messageModel) {
            case BROADCASTING:
                consumer.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.BROADCASTING);
                break;
            case CLUSTERING:
                consumer.setMessageModel(org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel.CLUSTERING);
                break;
            default:
                throw new IllegalArgumentException("Property 'messageModel' was wrong.");
        }

        switch (selectorType) {
            case TAG:
                consumer.subscribe(topic, selectorExpression);
                break;
            case SQL92:
                consumer.subscribe(topic, MessageSelector.bySql(selectorExpression));
                break;
            default:
                throw new IllegalArgumentException("Property 'selectorType' was wrong.");
        }

        switch (consumeMode) {
            case ORDERLY:
                consumer.setMessageListener(new DefaultMessageListenerOrderly());
                break;
            case CONCURRENTLY:
                consumer.setMessageListener(new DefaultMessageListenerConcurrently());
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }

        //if String is not is equal "true" TLS mode will represent the as default value false
        consumer.setUseTLS(new Boolean(tlsEnable));

        if (rocketMQListener instanceof RocketMQPushConsumerLifecycleListener) {
            ((RocketMQPushConsumerLifecycleListener) rocketMQListener).prepareStart(consumer);
        } else if (rocketMQReplyListener instanceof RocketMQPushConsumerLifecycleListener) {
            ((RocketMQPushConsumerLifecycleListener) rocketMQReplyListener).prepareStart(consumer);
        }

    }

}
