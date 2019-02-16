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
package com.codingapi.txlcn.tm.txmsg.transaction;

import com.codingapi.txlcn.common.exception.TransactionException;
import com.codingapi.txlcn.common.exception.TxManagerException;
import com.codingapi.txlcn.common.util.Transactions;
import com.codingapi.txlcn.logger.TxLogger;
import com.codingapi.txlcn.txmsg.RpcClient;
import com.codingapi.txlcn.tm.core.DTXContext;
import com.codingapi.txlcn.tm.core.DTXContextRegistry;
import com.codingapi.txlcn.tm.core.TransactionManager;
import com.codingapi.txlcn.tm.txmsg.RpcExecuteService;
import com.codingapi.txlcn.tm.txmsg.TransactionCmd;
import com.codingapi.txlcn.txmsg.params.JoinGroupParams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * Description:
 * Date: 2018/12/11
 *
 * @author ujued
 */
@Service("rpc_join-group")
@Slf4j
public class JoinGroupExecuteService implements RpcExecuteService {

    private final TransactionManager transactionManager;

    private final DTXContextRegistry dtxContextRegistry;

    private final TxLogger txLogger;

    private final RpcClient rpcClient;


    @Autowired
    public JoinGroupExecuteService(TxLogger txLogger, TransactionManager transactionManager,
                                   DTXContextRegistry dtxContextRegistry, RpcClient rpcClient) {
        this.txLogger = txLogger;
        this.transactionManager = transactionManager;
        this.dtxContextRegistry = dtxContextRegistry;
        this.rpcClient = rpcClient;
    }


    @Override
    public Serializable execute(TransactionCmd transactionCmd) throws TxManagerException {
        try {
            /**
             * 加入事务组 分布式事务的执行者 请求加入事务组
             * 根据事务组Id new一个DefaultDTXContext 对象 参数为 groupId FastStorage(Manager cache)
             */
            DTXContext dtxContext = dtxContextRegistry.get(transactionCmd.getGroupId());
            //获取加入事务组的参数
            JoinGroupParams joinGroupParams = transactionCmd.getMsg().loadBean(JoinGroupParams.class);
            /**
             * 记录日志
             */
            txLogger.transactionInfo(transactionCmd.getGroupId(), joinGroupParams.getUnitId(), "start join group");
            /**
             * 开始加入事务组
             */
            transactionManager.join(dtxContext, joinGroupParams.getUnitId(), joinGroupParams.getUnitType(),
                    rpcClient.getAppName(transactionCmd.getRemoteKey()), joinGroupParams.getTransactionState());
            //加入日志
            txLogger.transactionInfo(transactionCmd.getGroupId(), joinGroupParams.getUnitId(), "over join group");
        } catch (TransactionException e) {
            txLogger.error(this.getClass().getSimpleName(), e.getMessage());
            throw new TxManagerException(e.getLocalizedMessage());
        }
        // non response
        return null;
    }
}
