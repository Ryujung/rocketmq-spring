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

import org.apache.rocketmq.spring.support.RocketMQMessageConverter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 消息转换器，在RocketMQAutoConfiguration配置之前进行配置
 *
 * @see RocketMQMessageConverter
 */
@Configuration
@ConditionalOnMissingBean(RocketMQMessageConverter.class)
class MessageConverterConfiguration {

    @Bean
    public RocketMQMessageConverter createRocketMQMessageConverter() {
        return new RocketMQMessageConverter();
    }

}
