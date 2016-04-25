package com.ai.baas.smc.check.topology.test;

import com.ai.baas.smc.preprocess.topology.core.constant.SmcConstants.NameSpace;
import com.ai.opt.sdk.cache.factory.CacheClientFactory;
import com.ai.paas.ipaas.mcs.interfaces.ICacheClient;

public class TestClient {
    private static ICacheClient countCacheClient;

    public static void main(String[] args) {

        countCacheClient = CacheClientFactory.getCacheClient(NameSpace.CHECK_COUNT_CACHE);
    }

}
