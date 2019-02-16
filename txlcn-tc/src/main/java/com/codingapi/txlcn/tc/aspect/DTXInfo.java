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
package com.codingapi.txlcn.tc.aspect;

import com.codingapi.txlcn.tc.annotation.DTXPropagation;
import com.codingapi.txlcn.common.util.Transactions;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.aopalliance.intercept.MethodInvocation;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;

/**
 * Description:
 * Date: 19-1-11 下午1:21
 *
 * @author ujued
 */
@AllArgsConstructor
@Data
public class DTXInfo {
    private static final Map<String, DTXInfo> dtxInfoCache = new ConcurrentReferenceHashMap<>();  //全局缓存 存放事务单元

    private String transactionType;  //事务类型

    private DTXPropagation transactionPropagation;  //事务的传播机制

    private TransactionInfo transactionInfo;  //事务信息

    /**
     * 用户实例对象的业务方法（包含注解信息）
     */
    private Method businessMethod;

    private String unitId;

    private DTXInfo(Method method, Object[] args, Class<?> targetClass) {
        this.transactionInfo = new TransactionInfo();
        this.transactionInfo.setTargetClazz(targetClass);  //事务执行器  class
        this.transactionInfo.setArgumentValues(args);    //参数值数组
        this.transactionInfo.setMethod(method.getName()); //方法
        this.transactionInfo.setMethodStr(method.toString()); //方法字符串
        this.transactionInfo.setParameterTypes(method.getParameterTypes()); //参数类型

        this.businessMethod = method;
        this.unitId = Transactions.unitId(method.toString()); //方法签名生成事务单元ID
    }

    private void reanalyseMethodArgs(Object[] args) {
        this.transactionInfo.setArgumentValues(args);
    }

    public static DTXInfo getFromCache(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        //
        String signature = proceedingJoinPoint.getSignature().toString();
        String unitId = Transactions.unitId(signature);
        DTXInfo dtxInfo = dtxInfoCache.get(unitId);
        if (Objects.isNull(dtxInfo)) {
            MethodSignature methodSignature = (MethodSignature) proceedingJoinPoint.getSignature();
            Method method = methodSignature.getMethod();
            Class<?> targetClass = proceedingJoinPoint.getTarget().getClass();
            Method thisMethod = targetClass.getMethod(method.getName(), method.getParameterTypes());
            dtxInfo = new DTXInfo(thisMethod, proceedingJoinPoint.getArgs(), targetClass);
            dtxInfoCache.put(unitId, dtxInfo);
        }
        dtxInfo.reanalyseMethodArgs(proceedingJoinPoint.getArgs());
        return dtxInfo;
    }

    public static DTXInfo getFromCache(MethodInvocation methodInvocation) {
        String signature = methodInvocation.getMethod().toString();
        String unitId = Transactions.unitId(signature);
        DTXInfo dtxInfo = dtxInfoCache.get(unitId);
        if (Objects.isNull(dtxInfo)) {
            dtxInfo = new DTXInfo(methodInvocation.getMethod(),
                    methodInvocation.getArguments(), methodInvocation.getThis().getClass());
            dtxInfoCache.put(unitId, dtxInfo);
        }
        dtxInfo.reanalyseMethodArgs(methodInvocation.getArguments());
        return dtxInfo;
    }

    public static DTXInfo getFromCache(Method method, Object[] args, Class<?> targetClass) {
        String signature = method.getName();
        String unitId = Transactions.unitId(signature);
        DTXInfo dtxInfo = dtxInfoCache.get(unitId);
        if (Objects.isNull(dtxInfo)) {
            dtxInfo = new DTXInfo(method, args, targetClass);
            dtxInfoCache.put(unitId, dtxInfo);
        }
        dtxInfo.reanalyseMethodArgs(args);
        return dtxInfo;
    }
}