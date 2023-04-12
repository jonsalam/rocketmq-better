package com.clouditora.mq.network.coord;

import com.clouditora.mq.common.service.AbstractScheduledService;
import com.clouditora.mq.network.util.CoordinatorUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Getter
public class CommandCleaner extends AbstractScheduledService implements CallbackExecutor {
    protected final ConcurrentHashMap<Integer, CommandFuture> commandMap;
    protected ExecutorService callbackExecutor;

    public CommandCleaner(ConcurrentHashMap<Integer, CommandFuture> commandMap, ExecutorService callbackExecutor) {
        super(TimeUnit.MILLISECONDS, 3000, 1000);
        this.commandMap = commandMap;
        this.callbackExecutor = callbackExecutor;
    }

    public CommandCleaner(long initialDelay, long delay, ConcurrentHashMap<Integer, CommandFuture> commandMap) {
        super(TimeUnit.MILLISECONDS, initialDelay, delay);
        this.commandMap = commandMap;
    }

    @Override
    public String getServiceName() {
        return "CommandCleaner";
    }

    @Override
    protected void run() {
        cleanTimeoutCommand();
    }

    /**
     * 清理超时的请求
     * This method is periodically invoked to scan and expire deprecated request.
     *
     * @link org.apache.rocketmq.remoting.netty.NettyRemotingAbstract#scanResponseTable
     */
    protected void cleanTimeoutCommand() {
        List<CommandFuture> timeoutList = new LinkedList<>();
        Iterator<Map.Entry<Integer, CommandFuture>> it = this.commandMap.entrySet().iterator();
        while (it.hasNext()) {
            CommandFuture command = it.next().getValue();
            if (System.currentTimeMillis() - command.getBeginTime() >= command.getTimeout() + 1000) {
                // 超时啦
                it.remove();
                timeoutList.add(command);
                log.warn("remove timeout request: {}", command);
            }
        }

        for (CommandFuture response : timeoutList) {
            try {
                CoordinatorUtil.invokeCallback(response, getCallbackExecutor());
            } catch (Throwable e) {
                log.warn("cleanTimeoutRequest Exception", e);
            }
        }
    }
}
