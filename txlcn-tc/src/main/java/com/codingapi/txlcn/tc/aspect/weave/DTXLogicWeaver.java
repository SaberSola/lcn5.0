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
package com.codingapi.txlcn.tc.aspect.weave;

import com.codingapi.txlcn.tc.aspect.DTXInfo;
import com.codingapi.txlcn.tc.core.DTXLocalContext;
import com.codingapi.txlcn.tc.core.DTXServiceExecutor;
import com.codingapi.txlcn.tc.core.TxTransactionInfo;
import com.codingapi.txlcn.tc.core.context.TCGlobalContext;
import com.codingapi.txlcn.tc.core.context.TxContext;
import com.codingapi.txlcn.tracing.TracingContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * Description:
 * Company: CodingApi
 * Date: 2018/11/29
 *
 * @author ujued
 */
@Component
@Slf4j
public class DTXLogicWeaver {

    private final DTXServiceExecutor transactionServiceExecutor;   //分布式事务业务执行器

    private final TCGlobalContext globalContext;

    @Autowired
    public DTXLogicWeaver(DTXServiceExecutor transactionServiceExecutor, TCGlobalContext globalContext) {
        this.transactionServiceExecutor = transactionServiceExecutor;
        this.globalContext = globalContext;
    }

    public Object runTransaction(DTXInfo dtxInfo, BusinessCallback business) throws Throwable {

        if (Objects.isNull(DTXLocalContext.cur())) {
            DTXLocalContext.getOrNew();
        } else {
            return business.call();
        }

        log.debug("<---- TxLcn start ---->");
        DTXLocalContext dtxLocalContext = DTXLocalContext.getOrNew(); //创建一分布式事务远程控制对象
        TxContext txContext;  //事务的上下文
        if (globalContext.hasTxContext()) {
            // 有事务上下文的获取父上下 文
            txContext = globalContext.txContext(); // 参与者的话 直接获取发起者的
            dtxLocalContext.setInGroup(true);
            log.debug("Unit[{}] used parent's TxContext[{}].", dtxInfo.getUnitId(), txContext.getGroupId());
        } else {
            // 没有的开启本地事务上下文
            txContext = globalContext.startTx();
        }

        // 本地事务调用
        if (Objects.nonNull(dtxLocalContext.getGroupId())) {
            dtxLocalContext.setDestroy(false);
        }

        dtxLocalContext.setUnitId(dtxInfo.getUnitId());//设置事务的单元Id
        dtxLocalContext.setGroupId(txContext.getGroupId());//设置事务组Id
        dtxLocalContext.setTransactionType(dtxInfo.getTransactionType());//事务的类型

        // 事务参数
        TxTransactionInfo info = new TxTransactionInfo();
        info.setBusinessCallback(business);// 业务执行器
        info.setGroupId(txContext.getGroupId()); // 事务组Id
        info.setUnitId(dtxInfo.getUnitId()); //事务单元Id
        info.setPointMethod(dtxInfo.getBusinessMethod()); //切点方法
        info.setPropagation(dtxInfo.getTransactionPropagation()); // 事务的传播机制
        info.setTransactionInfo(dtxInfo.getTransactionInfo()); //事务切面信息
        info.setTransactionType(dtxInfo.getTransactionType()); //事务的的类型
        info.setTransactionStart(txContext.isDtxStart()); //是否是事务的发起者

        //LCN事务处理器
        try {
            //开始执行
            return transactionServiceExecutor.transactionRunning(info);
        } finally {
            if (dtxLocalContext.isDestroy()) {
                // 获取事务上下文通知事务执行完毕
                synchronized (txContext.getLock()) {
                    txContext.getLock().notifyAll();
                }

                // TxContext生命周期是？ 和事务组一样（不与具体模块相关的）
                if (!dtxLocalContext.isInGroup()) {
                    globalContext.destroyTx();
                }

                DTXLocalContext.makeNeverAppeared();
                TracingContext.tracing().destroy();
            }
            log.debug("<---- TxLcn end ---->");
        }
    }
}
