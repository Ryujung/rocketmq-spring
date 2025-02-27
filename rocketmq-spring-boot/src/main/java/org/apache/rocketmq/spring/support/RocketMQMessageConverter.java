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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.ArrayList;
import java.util.List;
import org.springframework.messaging.converter.ByteArrayMessageConverter;
import org.springframework.messaging.converter.CompositeMessageConverter;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.StringMessageConverter;
import org.springframework.util.ClassUtils;

/**
 * @see MessageConverter
 * @see CompositeMessageConverter
 */
public class RocketMQMessageConverter {

    private static final boolean JACKSON_PRESENT;
    private static final boolean FASTJSON_PRESENT;

    static {
        ClassLoader classLoader = RocketMQMessageConverter.class.getClassLoader();
        // 判断项目中是否存在jackson的依赖，如果存在将会进行延迟加载，返回true，如果不存在，返回false
        JACKSON_PRESENT =
            ClassUtils.isPresent("com.fasterxml.jackson.databind.ObjectMapper", classLoader) &&
                ClassUtils.isPresent("com.fasterxml.jackson.core.JsonGenerator", classLoader);
        // 判断项目中是否存在fastjson的依赖，如果存在将会进行延迟加载，返回true，如果不存在，返回false
        FASTJSON_PRESENT = ClassUtils.isPresent("com.alibaba.fastjson.JSON", classLoader) &&
            ClassUtils.isPresent("com.alibaba.fastjson.support.config.FastJsonConfig", classLoader);
    }

    private final CompositeMessageConverter messageConverter;

    /**
     * 根据依赖的序列化工具，初始化 CompositeMessageConverter
     */
    public RocketMQMessageConverter() {
        List<MessageConverter> messageConverters = new ArrayList<>();
        ByteArrayMessageConverter byteArrayMessageConverter = new ByteArrayMessageConverter();
        byteArrayMessageConverter.setContentTypeResolver(null);
        messageConverters.add(byteArrayMessageConverter);
        messageConverters.add(new StringMessageConverter());
        if (JACKSON_PRESENT) {
            MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
            ObjectMapper mapper = converter.getObjectMapper();
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            mapper.registerModule(new JavaTimeModule());
            converter.setObjectMapper(mapper);
            messageConverters.add(converter);
        }
        if (FASTJSON_PRESENT) {
            try {
                messageConverters.add(
                    (MessageConverter)ClassUtils.forName(
                        "com.alibaba.fastjson.support.spring.messaging.MappingFastJsonMessageConverter",
                        ClassUtils.getDefaultClassLoader()).newInstance());
            } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ignored) {
                //ignore this exception
            }
        }
        messageConverter = new CompositeMessageConverter(messageConverters);
    }

    public MessageConverter getMessageConverter() {
        return messageConverter;
    }

}
