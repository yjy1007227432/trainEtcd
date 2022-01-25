package com.train.etcd.etcd.service;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.train.etcd.etcd.config.JetcdClient;
import com.train.etcd.etcd.module.JetcdParam;
import com.train.etcd.etcd.module.JetcdResp;
import com.train.etcd.etcd.module.LockData;
import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import io.etcd.jetcd.watch.WatchEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

@Component
@ConditionalOnExpression("${module.jetcd.enable:false}")
public class EtcdModule{

    @Resource
    private JetcdClient jetcdClient;

    private static final Logger logger = LoggerFactory.getLogger(EtcdModule.class);

    //锁路径，方便记录日志
    private String lockPath;

    //定时任务线程池
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    //线程与锁对象的映射
    private final ConcurrentMap<Thread, LockData> threadData = Maps.newConcurrentMap();


    /**
     * 加锁
     * @param jetcdParam
     */
    public void lock(JetcdParam jetcdParam){
        Thread currentThread = Thread.currentThread();
        LockData existsLockData = threadData.get(currentThread);
        logger.info(currentThread.getName() + " 加锁 existsLockData：" + existsLockData);
        Lease leaseClient = jetcdClient.getLease();
        Lock lockClient = jetcdClient.getLock();
        //锁重入
        if (existsLockData != null && existsLockData.isLockSuccess()) {
            int lockCount = existsLockData.lockCount.incrementAndGet();
            if (lockCount < 0) {
                throw new Error("超出etcd锁可重入次数限制");
            }
            return;
        }
        //创建租约，记录租约id
        long leaseId;
        try {
            long leaseTTL = jetcdParam.getLeaseTTL();
            leaseId = leaseClient.grant(TimeUnit.NANOSECONDS.toSeconds(leaseTTL)).get().getID();
            //续租心跳周期
            long period = leaseTTL - leaseTTL / 5;
            //启动定时续约
            //续约锁租期的定时任务，初次启动延迟，默认为1s，根据实际业务需要设置
            long initialDelay = 0L;
            scheduledExecutorService.scheduleAtFixedRate(() -> {
                        try {
                            logger.info("租约续期。。。");
                            leaseClient.keepAliveOnce(leaseId);
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                    },
                    initialDelay,
                    period,
                    TimeUnit.NANOSECONDS);

            //加锁
            LockResponse lockResponse = lockClient.lock(ByteSequence.from(jetcdParam.getLock_key().getBytes()), leaseId).get();
            if (lockResponse != null) {
                lockPath = lockResponse.getKey().toString(StandardCharsets.UTF_8);
                logger.info("线程：{} 加锁成功，锁路径：{}", currentThread.getName(), lockPath);
            }

            //加锁成功，设置锁对象
            LockData lockData = new LockData(jetcdParam.getLock_key(), currentThread);
            lockData.lockCount.incrementAndGet();
            lockData.setLeaseId(leaseId);
            lockData.setService(scheduledExecutorService);
            threadData.put(currentThread, lockData);
            lockData.setLockSuccess(true);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 检查
     * @param jetcdParam
     */
    /**
     * 检查
     * @param jetcdParam
     */
    public boolean check(JetcdParam jetcdParam){
        Thread currentThread = Thread.currentThread();
        logger.info(currentThread.getName() + " 释放锁..");
        LockData lockData = threadData.get(currentThread);
        logger.info(currentThread.getName() + " lockData " + lockData);
        if (lockData == null) {
            logger.info("线程：" + currentThread.getName() + " 没有获得锁，lockKey：" + jetcdParam.getLock_key());
            return true;
        }else{
            return lockData.isLockSuccess();
        }
    }


    /**
     * 解锁
     * @param jetcdParam
     */
    /**
     * 解锁
     * @param jetcdParam
     */
    public void unlock(JetcdParam jetcdParam){
        Lease leaseClient = jetcdClient.getLease();
        Lock lockClient = jetcdClient.getLock();
        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        logger.info(currentThread.getName() + " lockData " + lockData);
        if (lockData == null) {
            throw new IllegalMonitorStateException("线程：" + currentThread.getName() + " 没有获得锁，lockKey：" + jetcdParam.getLock_key());
        }
        int lockCount = lockData.lockCount.decrementAndGet();
        if (lockCount > 0) {
            return;
        }
        if (lockCount < 0) {
            throw new IllegalMonitorStateException("线程：" + currentThread.getName() + " 锁次数为负数，lockKey：" + jetcdParam.getLock_key());
        }
        try {
            //正常释放锁
            if (lockPath != null) {
                lockClient.unlock(ByteSequence.from(lockPath.getBytes())).get();
            }
            //关闭续约的定时任务
            lockData.getService().shutdown();
            //删除租约
            if (lockData.getLeaseId() != 0L) {
                leaseClient.revoke(lockData.getLeaseId());
            }
        } catch (InterruptedException | ExecutionException e) {
            //e.printStackTrace();
            logger.error("线程：" + currentThread.getName() + "解锁失败。", e);
        } finally {
            //移除当前线程资源
            threadData.remove(currentThread);
        }
        logger.info("线程：{} 释放锁", currentThread.getName());
    }


    /**
     * 创建服务监听
     * @param jetcdParam
     */
    /**
     * 创建服务监听
     * @param jetcdParam
     */
    public JetcdResp createService(JetcdParam jetcdParam){
        JetcdResp jetcdResp = new JetcdResp();
        long leaseId= 0L;
        try {
            Long leaseTTL = jetcdParam.getLeaseTTL();
            Lease leaseClient = jetcdClient.getLease();
            KV kvClient = jetcdClient.getKv();
            Watch watchClient = jetcdClient.getWatch();
            ByteSequence key_temp = ByteSequence.from(jetcdParam.getLock_key().getBytes(StandardCharsets.UTF_8));
            ByteSequence value_temp = ByteSequence.from("[]".getBytes(StandardCharsets.UTF_8));
            if (leaseTTL!=null&&leaseTTL>0) {
                leaseId = leaseClient.grant(leaseTTL).get().getID();
                logger.debug("leaseId=={}", leaseId);
                //续租心跳周期
                long period = leaseTTL - leaseTTL / 5;
                //启动定时续约
                //续约锁租期的定时任务，初次启动延迟，默认为1s，根据实际业务需要设置
                long initialDelay = 0L;
                long finalLeaseId = leaseId;
                scheduledExecutorService.scheduleAtFixedRate(() -> {
                            try {
                                logger.debug("租约续期，leaseId=={}", finalLeaseId);
                                leaseClient.keepAliveOnce(finalLeaseId);
                            } catch (Exception e) {
                                System.out.println(e.getMessage());
                            }
                        },
                        initialDelay,
                        period,
                        TimeUnit.SECONDS);

                kvClient.put(key_temp, value_temp, PutOption.newBuilder().withLeaseId(leaseId).build());
            }else{
                kvClient.put(key_temp,value_temp);
            }
            Boolean enWatch = jetcdParam.getEnWatch();
            if (enWatch!=null&&enWatch) {
                watchClient.watch(key_temp, watchResponse -> {
                    try {
                        List<WatchEvent> events = watchResponse.getEvents();
                        for (WatchEvent event : events) {
                            KeyValue keyValue = event.getKeyValue();
                            logger.debug("lease=={},key=={},cv=={},v=={},mv=={}", keyValue.getLease(),
                                    keyValue.getKey(),
                                    keyValue.getCreateRevision(),
                                    keyValue.getVersion(),
                                    keyValue.getModRevision());
                            ByteSequence value = keyValue.getValue();
                            String vals= value.toString(StandardCharsets.UTF_8);
                            Set<Object> se = JSONObject.parseObject(vals, HashSet.class);
                            for (Object o : se) {
                                JSONObject map = (JSONObject)o;
                                String name = map.getString("name");
                                String health_check = map.getString("health_check");
                                String service_url = map.getString("service_url");
                                logger.info("name = {},health_check = {},service_url = {}",name,health_check,service_url);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("监听时发现异常{}",e.getMessage());
                    }
                });
            }
            Map<String,Object> data = new HashMap<>();
            data.put("leaseId",leaseId);
            data.put("lock_kcy",jetcdParam.getLock_key());
            jetcdResp.setData(data);
            jetcdResp.setDesc(Boolean.TRUE.equals(enWatch) ?"服务已创建已开启监听":"服务已创建未开启监听");
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            jetcdResp.setData(leaseId);
            jetcdResp.setDesc("服务已创建失败！");
            return jetcdResp;
        }

        return jetcdResp;
    }


    /**
     * 注册
     * @param jetcdParam
     */
    /**
     * 注册
     * @param jetcdParam
     */
    public JetcdResp register(JetcdParam jetcdParam){
        JetcdResp jetcdResp = new JetcdResp();
        try {
            String lock_key = jetcdParam.getLock_key();
            KV kv = jetcdClient.getKv();
            ByteSequence from = ByteSequence.from(lock_key.getBytes(StandardCharsets.UTF_8));
            CompletableFuture<GetResponse> getResponseCompletableFuture = kv.get(from, GetOption.newBuilder().isPrefix(false).build());
            GetResponse getResponse = getResponseCompletableFuture.get();
            List<KeyValue> kvs = getResponse.getKvs();
            KeyValue keyValueTemp = null;
            for (KeyValue keyValue : kvs) {
                ByteSequence key = keyValue.getKey();
                if (key.equals(from)){
                    keyValueTemp = keyValue;
                }
            }
            assert keyValueTemp != null;
            long leaseId = keyValueTemp.getLease();
            HashSet hashSet = JSONObject.parseObject(keyValueTemp.getValue().toString(StandardCharsets.UTF_8), HashSet.class);
            hashSet.add(jetcdParam.getValue());
            String jsonString = JSONObject.toJSONString(hashSet);
            ByteSequence to = ByteSequence.from(jsonString.getBytes(StandardCharsets.UTF_8));
            PutResponse putResponse = kv.put(from, to, PutOption.newBuilder().withLeaseId(leaseId).build()).get();
            jetcdResp.setDesc("服务注册成功");
            int size = hashSet.size();
            logger.info("已注册服务{}个",size);
            jetcdResp.setData(putResponse);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            jetcdResp.setDesc("服务注册失败");
        }
        return jetcdResp;
    }


    /**
     * 服务发现
     * @param jetcdParam
     */
    /**
     * 服务发现
     * @param jetcdParam
     */
    public JetcdResp serviceFind(JetcdParam jetcdParam){
        JetcdResp jetcdResp = new JetcdResp();
        try {
            String lock_key = jetcdParam.getLock_key();
            KV kv = jetcdClient.getKv();
            ByteSequence key = ByteSequence.from(lock_key.getBytes(StandardCharsets.UTF_8));
            List<KeyValue> kvs = kv.get(key, GetOption.newBuilder().isPrefix(true).build()).get().getKvs();
            jetcdResp.setDesc("发现服务");
            jetcdResp.setData(kvs);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("发生异常{}",e.getMessage());
            jetcdResp.setDesc(e.getMessage());

        }
        return jetcdResp;
    }

}
