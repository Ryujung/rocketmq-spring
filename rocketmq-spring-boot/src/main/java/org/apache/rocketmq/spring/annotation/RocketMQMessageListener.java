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

package org.apache.rocketmq.spring.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.LinkedBlockingQueue;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RocketMQMessageListener {

    String NAME_SERVER_PLACEHOLDER = "${rocketmq.name-server:}";
    String ACCESS_KEY_PLACEHOLDER = "${rocketmq.consumer.access-key:}";
    String SECRET_KEY_PLACEHOLDER = "${rocketmq.consumer.secret-key:}";
    String TRACE_TOPIC_PLACEHOLDER = "${rocketmq.consumer.customized-trace-topic:}";
    String ACCESS_CHANNEL_PLACEHOLDER = "${rocketmq.access-channel:}";

    /**
     * 消费组名称，同组内的消费者的消费逻辑必须相同，必填且全局唯一
     *
     * Apache RocketMQ 是按照消费者分组粒度管理订阅关系，因此，同一消费者分组内的消费者在消费逻辑上必须保持一致，
     * 否则会出现消费冲突，导致部分消息消费异常。
     *
     * Consumers of the same role is required to have exactly same subscriptions and consumerGroup to correctly achieve
     * load balance. It's required and needs to be globally unique.
     *
     *
     * See <a href="http://rocketmq.apache.org/docs/core-concept/">here</a> for further discussion.
     */
    String consumerGroup();

    /**
     * 消费主题
     *
     * Topic name.
     */
    String topic();

    /**
     * 消息过滤模式，支持Tag标签过滤和SQL属性过滤，默认为tag标签过滤，这两种过滤方式
     *
     * Control how to selector message.
     *
     * @see SelectorType
     */
    SelectorType selectorType() default SelectorType.TAG;

    /**
     * 过滤表达式，默认是不过滤，接收topic内所有类型的消息
     *
     * Control which message can be select. Grammar please see {@link SelectorType#TAG} and {@link SelectorType#SQL92}
     */
    String selectorExpression() default "*";

    /**
     * 消费模式，分为并发消费和顺序消费，默认为并发消费，性能更高
     *
     * Control consume mode, you can choice receive message concurrently or orderly.
     */
    ConsumeMode consumeMode() default ConsumeMode.CONCURRENTLY;

    /**
     * 消息模式，分为集群模式和广播模式，默认为集群模式
     *
     * Control message mode, if you want all subscribers receive all message, broadcasting is a good choice.
     */
    MessageModel messageModel() default MessageModel.CLUSTERING;

    /**
     * 消费者最大线程数
     *
     * Max consumer thread number.
     * @deprecated This property is not work well, because the consumer thread pool executor use
     * {@link LinkedBlockingQueue} with default capacity bound (Integer.MAX_VALUE), use
     * {@link RocketMQMessageListener#consumeThreadNumber} .
     * @see <a href="https://github.com/apache/rocketmq-spring/issues/429">issues#429</a>
     */
    @Deprecated
    int consumeThreadMax() default 64;

    /**
     * consumer thread number.
     */
    int consumeThreadNumber() default 20;

    /**
     * Max re-consume times.
     *
     * In concurrently mode, -1 means 16;
     * In orderly mode, -1 means Integer.MAX_VALUE.
     */
    int maxReconsumeTimes() default -1;

    /**
     * Maximum amount of time in minutes a message may block the consuming thread.
     */
    long consumeTimeout() default 15L;

    /**
     * Timeout for sending reply messages.
     */
    int replyTimeout() default 3000;

    /**
     * The property of "access-key".
     */
    String accessKey() default ACCESS_KEY_PLACEHOLDER;

    /**
     * The property of "secret-key".
     */
    String secretKey() default SECRET_KEY_PLACEHOLDER;

    /**
     * Switch flag instance for message trace.
     */
    boolean enableMsgTrace() default false;

    /**
     * The name value of message trace topic.If you don't config,you can use the default trace topic name.
     */
    String customizedTraceTopic() default TRACE_TOPIC_PLACEHOLDER;

    /**
     * The property of "name-server".
     */
    String nameServer() default NAME_SERVER_PLACEHOLDER;

    /**
     * The property of "access-channel".
     */
    String accessChannel() default ACCESS_CHANNEL_PLACEHOLDER;
 
    /**
     * The property of "tlsEnable" default false.
     */
    String tlsEnable() default "false";

    /**
     * The namespace of consumer.
     */
    String namespace() default "";

    /**
     * Message consume retry strategy in concurrently mode.
     *
     * -1,no retry,put into DLQ directly
     * 0,broker control retry frequency
     * >0,client control retry frequency
     */
    int delayLevelWhenNextConsume() default 0;

    /**
     * The interval of suspending the pull in orderly mode, in milliseconds.
     *
     * The minimum value is 10 and the maximum is 30000.
     */
    int suspendCurrentQueueTimeMillis() default 1000;

    /**
     * Maximum time to await message consuming when shutdown consumer, in milliseconds.
     * The minimum value is 0
     */
    int awaitTerminationMillisWhenShutdown() default 1000;

    /**
     * The property of "instanceName".
     */
    String instanceName() default "DEFAULT";
}
