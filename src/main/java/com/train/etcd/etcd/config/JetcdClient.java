package com.train.etcd.etcd.config;

import io.etcd.jetcd.*;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lease.LeaseKeepAliveResponse;
import io.etcd.jetcd.lease.LeaseTimeToLiveResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.options.LeaseOption;
import io.etcd.jetcd.options.PutOption;
import io.grpc.stub.StreamObserver;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;






@Component
@Data
public class JetcdClient {

    @Value("#{'${module.jetcd.endpoints:}'.split(',')}")
    private String[] endpoints;

    private Client client;

    private KV kv;

    private Lock lock;

    private Lease lease;

    private Watch watch;

    private Election election;

    @PostConstruct
    public void init() {
        client= Client.builder().endpoints(endpoints).build();
        kv=client.getKVClient();
        lock=client.getLockClient();
        lease=client.getLeaseClient();
        watch=client.getWatchClient();
        election=client.getElectionClient();
    }

    @PreDestroy
    public void destroy() {
        election.close();
        kv.close();
        lease.close();
        watch.close();
        lock.close();
        client.close();
    }

        // getter() & setter() ...
}
