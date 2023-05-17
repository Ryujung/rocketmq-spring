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

package org.apache.rocketmq.spring.autoconfigure;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.apache.rocketmq.spring.core.RocketMQReplyListener;
import org.apache.rocketmq.spring.support.DefaultRocketMQListenerContainer;
import org.apache.rocketmq.spring.support.RocketMQMessageConverter;
import org.apache.rocketmq.spring.support.RocketMQUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.StringUtils;

@Configuration
public class ListenerContainerConfiguration implements ApplicationContextAware {
    private final static Logger log = LoggerFactory.getLogger(ListenerContainerConfiguration.class);

    private ConfigurableApplicationContext applicationContext;

    private AtomicLong counter = new AtomicLong(0);

    private ConfigurableEnvironment environment;

    private RocketMQProperties rocketMQProperties;

    private RocketMQMessageConverter rocketMQMessageConverter;

    /**
     * 构造器方式注入bean：消息转换器、环境变量、RocketMQ配置信息
     */
    public ListenerContainerConfiguration(RocketMQMessageConverter rocketMQMessageConverter,
        ConfigurableEnvironment environment, RocketMQProperties rocketMQProperties) {
        this.rocketMQMessageConverter = rocketMQMessageConverter;
        this.environment = environment;
        this.rocketMQProperties = rocketMQProperties;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

    /**
     * 将有{@link RocketMQMessageListener}注解的单例bean注册到监听器容器中
     *
     * 这段代码的作用是:通过检查`rocketMQProperties`中的配置,判断指定Consumer Group和Topic的Listener是否被启用。
     * 如果未启用,则忽略Listener的初始化。
     *
     * 以具体例子来说明:假设`rocketMQProperties`配置如下:
     * # yaml文件
     * rocketmq:
     *   consumer:
     *     listeners:
     *       consumer-group1:
     *         topic1: true
     *         topic2: false
     *       consumer-group2:
     *
     * 那么:
     *   - listenerEnabled = rocketMQProperties.getConsumer().getListeners().getOrDefault("consumer-group1","topic1",true)
     *   - 这里`getOrDefault("consumer-group1","topic1",true)`的含义是,获取"consumer-group1"下的"topic1"配置,如果没有配置则默认为true。
     *   - 所以,listenerEnabled为true,表示"consumer-group1"监听"topic1"被启用。- listenerEnabled = rocketMQProperties.getConsumer().getListeners().getOrDefault("consumer-group1","topic2",true)
     *   - 这里会获取到"consumer-group1"下的"topic2"配置为false,所以listenerEnabled为false。
     *   - 表示"consumer-group1"监听"topic2"被禁用。- listenerEnabled = rocketMQProperties.getConsumer().getListeners().getOrDefault("consumer-group2","topic1",true)
     *   - 因为"consumer-group2"没有任何topic配置,所以会使用默认值true。
     *   - 表示"consumer-group2"监听任何topic都被启用(没有显式禁用任何topic)。
     * 所以,这段代码的主要作用是,检查Listener配置,如果某Consumer Group对某Topic被禁用了,则避免初始化该Listener,以免启动时报错。
     * 通过默认值和多层getOrDefault,实现了Listener配置的 fallback 机制,比较优雅地处理了全局配置与局部配置的关系。
     */
    public void registerContainer(String beanName, Object bean, RocketMQMessageListener annotation) {
        Class<?> clazz = AopProxyUtils.ultimateTargetClass(bean);

        // 有且只能有一个监听器类型的实现， RocketMQListener（无回复） 或 RocketMQReplyListener（有回复）
        if (RocketMQListener.class.isAssignableFrom(bean.getClass()) && RocketMQReplyListener.class.isAssignableFrom(bean.getClass())) {
            throw new IllegalStateException(clazz + " cannot be both instance of " + RocketMQListener.class.getName() + " and " + RocketMQReplyListener.class.getName());
        }
        if (!RocketMQListener.class.isAssignableFrom(bean.getClass()) && !RocketMQReplyListener.class.isAssignableFrom(bean.getClass())) {
            throw new IllegalStateException(clazz + " is not instance of " + RocketMQListener.class.getName() + " or " + RocketMQReplyListener.class.getName());
        }

        // 将注解占位符通过环境配置进行解析，例如将@RocketMQMessageListener(topic= "${rocketmq.xxx}") 解析为配置文件中 rocketmq.xxx 配置的值
        String consumerGroup = this.environment.resolvePlaceholders(annotation.consumerGroup());
        String topic = this.environment.resolvePlaceholders(annotation.topic());

        // 获取配置的消费者所在的 消费者组信息map 中的 键为注解的topicName的值，代表是否开启被监听
        boolean listenerEnabled =
            (boolean) rocketMQProperties.getConsumer().getListeners().getOrDefault(consumerGroup, Collections.EMPTY_MAP)
                .getOrDefault(topic, true);

        if (!listenerEnabled) {
            log.debug(
                "Consumer Listener (group:{},topic:{}) is not enabled by configuration, will ignore initialization.",
                consumerGroup, topic);
            return;
        }
        validate(annotation);

        // 构造XXXContainer_0,XXXContainer_1,...之类的容器名称
        String containerBeanName = String.format("%s_%s", DefaultRocketMQListenerContainer.class.getName(),
            counter.incrementAndGet());
        GenericApplicationContext genericApplicationContext = (GenericApplicationContext) applicationContext;

        // 注册bean并且使用自定义的构建方法 createRocketMQListenerContainer
        genericApplicationContext.registerBean(containerBeanName, DefaultRocketMQListenerContainer.class,
            () -> createRocketMQListenerContainer(containerBeanName, bean, annotation));
        DefaultRocketMQListenerContainer container = genericApplicationContext.getBean(containerBeanName,
            DefaultRocketMQListenerContainer.class);

        // 检查并启动监听器容器
        if (!container.isRunning()) {
            try {
                container.start();
            } catch (Exception e) {
                log.error("Started container failed. {}", container, e);
                throw new RuntimeException(e);
            }
        }

        log.info("Register the listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
    }

    /**
     * 构造一个新的监听器容器，并解析根据环境配置解析注解中的占位符，设置监听器的属性
     */
    private DefaultRocketMQListenerContainer createRocketMQListenerContainer(String name, Object bean,
        RocketMQMessageListener annotation) {
        DefaultRocketMQListenerContainer container = new DefaultRocketMQListenerContainer();

        container.setRocketMQMessageListener(annotation);

        String nameServer = environment.resolvePlaceholders(annotation.nameServer());
        nameServer = StringUtils.hasLength(nameServer) ? nameServer : rocketMQProperties.getNameServer();
        String accessChannel = environment.resolvePlaceholders(annotation.accessChannel());
        container.setNameServer(nameServer);
        if (StringUtils.hasLength(accessChannel)) {
            container.setAccessChannel(AccessChannel.valueOf(accessChannel));
        }
        container.setTopic(environment.resolvePlaceholders(annotation.topic()));
        String tags = environment.resolvePlaceholders(annotation.selectorExpression());
        if (StringUtils.hasLength(tags)) {
            container.setSelectorExpression(tags);
        }
        container.setConsumerGroup(environment.resolvePlaceholders(annotation.consumerGroup()));
        container.setTlsEnable(environment.resolvePlaceholders(annotation.tlsEnable()));
        if (RocketMQListener.class.isAssignableFrom(bean.getClass())) {
            container.setRocketMQListener((RocketMQListener) bean);
        } else if (RocketMQReplyListener.class.isAssignableFrom(bean.getClass())) {
            container.setRocketMQReplyListener((RocketMQReplyListener) bean);
        }
        container.setMessageConverter(rocketMQMessageConverter.getMessageConverter());
        container.setName(name);

        String namespace = environment.resolvePlaceholders(annotation.namespace());
        container.setNamespace(RocketMQUtil.getNamespace(namespace,
            rocketMQProperties.getConsumer().getNamespace()));
        return container;
    }

    /**
     * 广播类型消息不支持有序消费，检查注解是否存在逻辑错误
     */
    private void validate(RocketMQMessageListener annotation) {
        if (annotation.consumeMode() == ConsumeMode.ORDERLY &&
            annotation.messageModel() == MessageModel.BROADCASTING) {
            throw new BeanDefinitionValidationException(
                "Bad annotation definition in @RocketMQMessageListener, messageModel BROADCASTING does not support ORDERLY message!");
        }
    }
}
