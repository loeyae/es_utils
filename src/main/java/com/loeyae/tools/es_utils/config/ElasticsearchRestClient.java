package com.loeyae.tools.es_utils.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Objects;

/**
 * elastic search rest client.
 *
 * @date 2020-02-05
 * @version 1.0
 * @author zhangyi<loeyae@gmail.com>
 */
@Slf4j
@Configuration
public class ElasticsearchRestClient {
    private static final int ADDRESS_LENGTH = 2;
    private static final String HTTP_SCHEME = "http";

    /**
     * 使用冒号隔开ip和端口
     */
    @Value("${elasticsearch.cluster-nodes}")
    String[] ipAddress;

    /**
     * RestClientBuilder
     *
     * @return
     */
    @Bean
    public RestClientBuilder restClientBuilder() {
        HttpHost[] hosts = Arrays.stream(ipAddress)
                .map(this::makeHttpHost)
                .filter(Objects::nonNull)
                .toArray(HttpHost[]::new);
        log.debug("hosts:{}", Arrays.toString(hosts));
        return RestClient.builder(hosts);
    }

    /**
     * RestHighLevelClient
     *
     * @param restClientBuilder
     * @return
     */
    @Bean(name = "highLevelClient")
    public RestHighLevelClient highLevelClient(@Autowired RestClientBuilder restClientBuilder) {
        RestClient.FailureListener failureListener = new RestClient.FailureListener(){
            @Override
            public void onFailure(Node node) {
                super.onFailure(node);
            }
        };
        restClientBuilder.setFailureListener(failureListener);
        restClientBuilder.setMaxRetryTimeoutMillis(60000);
        return new RestHighLevelClient(restClientBuilder);
    }

    /**
     * makeHttpHost
     *
     * @param s
     * @return
     */
    private HttpHost makeHttpHost(String s) {
        assert StringUtils.isNotEmpty(s);
        String[] address = s.split(":");
        if (address.length == ADDRESS_LENGTH) {
            String ip = address[0];
            int port = Integer.parseInt(address[1]);
            return new HttpHost(ip, port, HTTP_SCHEME);
        } else {
            return null;
        }
    }
}