/*
 * Copyright 2017-2019 CodingApi .
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.codingapi.txlcn.tracing;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.codingapi.txlcn.common.util.Maps;
import com.codingapi.txlcn.common.util.id.RandomUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Base64Utils;
import org.springframework.util.StringUtils;

import java.nio.charset.Charset;
import java.util.*;

/**
 * Description:
 * 1. {@code fields}为 {@code null}。发起方出现，未开始事务组
 * 2. {@code fields}不为空，fields.get(TracingConstants.GROUP_ID) 是 {@code empty}。参与方出现，未开启事务组。
 * 3. TBD
 * Date: 19-1-28 下午4:21
 * 事务追踪类
 * @author ujued
 */
@Slf4j
public class TracingContext {

    private static ThreadLocal<TracingContext> tracingContextThreadLocal = new ThreadLocal<>(); //ThreadLocal存储

    private TracingContext() {

    }

    public static TracingContext tracing() {
        if (tracingContextThreadLocal.get() == null) {
            tracingContextThreadLocal.set(new TracingContext());
        }
        return tracingContextThreadLocal.get();  //获取追踪上下文
    }

    private Map<String, String> fields;

    public void beginTransactionGroup() {  //分布式事务开始
        if (hasGroup()) {
            return;
        }
        //初始化事务组Id key1 value2 key2 value2
        init(Maps.newHashMap(TracingConstants.GROUP_ID, RandomUtils.randomKey(), TracingConstants.APP_MAP, "{}"));
    }

    public void init(Map<String, String> initFields) {
        if (Objects.isNull(fields)) {
            this.fields = new HashMap<>();
        }
        //APP_MAP base64 解码
        if(initFields.containsKey(TracingConstants.APP_MAP)){
            String appMapVal = initFields.get(TracingConstants.APP_MAP);
            if(!appMapVal.startsWith("{")||!appMapVal.contains("{")){
                initFields.put(TracingConstants.APP_MAP,baseString2appMap(appMapVal));
            }
        }
        this.fields.putAll(initFields);
    }
    //判断是不是存在事务组
    //false 代表不存在事务组 则是事务的发起方
    //true 代表存在事务组 则是事务的参与方
    public boolean hasGroup() {
        return Objects.nonNull(fields) && fields.containsKey(TracingConstants.GROUP_ID) &&
                StringUtils.hasText(fields.get(TracingConstants.GROUP_ID));
    }

    public String groupId() {
        if (hasGroup()) {
            return fields.get(TracingConstants.GROUP_ID);
        }
        raiseNonGroupException();
        return "";
    }

    public void addApp(String serviceId, String address) {
        if (hasGroup()) {
            JSONObject map = JSON.parseObject(this.fields.get(TracingConstants.APP_MAP));
            if (map.containsKey(serviceId)) {
                return;
            }
            map.put(serviceId, address);
            this.fields.put(TracingConstants.APP_MAP, JSON.toJSONString(map));
            return;
        }
        raiseNonGroupException();
    }

    public String appMapBase64String() {
        if (hasGroup()) {
            return Base64Utils.encodeToString(this.fields.get(TracingConstants.APP_MAP).getBytes(Charset.forName("utf8")));
        }
        raiseNonGroupException();
        return "";
    }

    private String baseString2appMap(String base64Str) {
        //解码
        if(!"".equals(base64Str)){
            base64Str = Base64Utils.encodeToString(base64Str.getBytes(Charset.forName("utf8")));
        }
        return base64Str;
    }

    public JSONObject appMap() {
        if (hasGroup()) {
            String appMap = this.fields.get(TracingConstants.APP_MAP);
            log.debug("App map: {}", appMap);
            return JSON.parseObject(appMap);
        }
        raiseNonGroupException();
        return JSON.parseObject("{}");
    }

    public void destroy() {
        if (Objects.nonNull(tracingContextThreadLocal.get())) {
            tracingContextThreadLocal.set(null);
        }
    }

    private void raiseNonGroupException() {
        throw new IllegalStateException("non group id.");
    }

    public static void main(String[] args){
        TracingContext tracingContext = new TracingContext();
        System.out.println(tracingContext.baseString2appMap("{}"));
    }
}
