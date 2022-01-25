package com.train.etcd.etcd.module;

import lombok.Data;

import java.util.Map;

@Data
public class JetcdParam {

    /**
     * key
     */
    private String lock_key;

    /**
     * 租约过期时间
     */
    private Long leaseTTL;

    /**
     * value
     */
    private Map<String,Object> value;

    /**
     * 是否开启监听
     */
    private Boolean enWatch;

    /**
     * 租约Id
     */
    private Long leaseId;

    // getter() & setter() ...
}