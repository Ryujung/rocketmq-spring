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

import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.spring.annotation.RocketMQTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQLocalTransactionListener;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.AopProxyUtils;
import org.springframework.aop.scope.ScopedProxyUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;

/**
 * 事务配置类，在{@link RocketMQAutoConfiguration}之后进行配置
 */
@Configuration
public class RocketMQTransactionConfiguration implements ApplicationContextAware, SmartInitializingSingleton {

    private final static Logger log = LoggerFactory.getLogger(RocketMQTransactionConfiguration.class);

    private ConfigurableApplicationContext applicationContext;

    /**
     * {@link ApplicationContextAware}接口获取容器
     *
     * @param applicationContext spring容器
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

    /**
     * {@link SmartInitializingSingleton}接口,单例对象初始化回调,获取事务监听的单例bean
     * {@link ScopedProxyUtils#isScopedTarget(String)}方法用于检查给定的beanName是否是引用作用域代理的目标bean的名称。
     * 获取带有事务监听注解，且不是代理作用域的单例对象，注册为
     */
    @Override
    public void afterSingletonsInstantiated() {
        Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(RocketMQTransactionListener.class)
            .entrySet().stream().filter(entry -> !ScopedProxyUtils.isScopedTarget(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        beans.forEach(this::registerTransactionListener);
    }

    private void registerTransactionListener(String beanName, Object bean) {
        // 获取最终目标类的Class对象
        Class<?> clazz = AopProxyUtils.ultimateTargetClass(bean);

        // 如果不是{@link RocketMQLocalTransactionListener}接口的实现类,抛出非法状态异常
        if (!RocketMQLocalTransactionListener.class.isAssignableFrom(bean.getClass())) {
            throw new IllegalStateException(clazz + " is not instance of " + RocketMQLocalTransactionListener.class.getName());
        }
        // 一个TransactionMQProducer实例只能注册一个事务监听器
        RocketMQTransactionListener annotation = clazz.getAnnotation(RocketMQTransactionListener.class);
        RocketMQTemplate rocketMQTemplate = (RocketMQTemplate) applicationContext.getBean(annotation.rocketMQTemplateBeanName());
        if (((TransactionMQProducer) rocketMQTemplate.getProducer()).getTransactionListener() != null) {
            throw new IllegalStateException(annotation.rocketMQTemplateBeanName() + " already exists RocketMQLocalTransactionListener");
        }
        // 根据注解设置TransactionMQProducer的线程池核心参数
        ((TransactionMQProducer) rocketMQTemplate.getProducer()).setExecutorService(new ThreadPoolExecutor(annotation.corePoolSize(), annotation.maximumPoolSize(),
            annotation.keepAliveTime(), annotation.keepAliveTimeUnit(), new LinkedBlockingDeque<>(annotation.blockingQueueSize())));
        // 设置事务监听器并打印日志
        ((TransactionMQProducer) rocketMQTemplate.getProducer()).setTransactionListener(RocketMQUtil.convert((RocketMQLocalTransactionListener) bean));
        log.debug("RocketMQLocalTransactionListener {} register to {} success", clazz.getName(), annotation.rocketMQTemplateBeanName());
    }
}
