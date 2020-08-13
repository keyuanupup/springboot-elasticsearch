package com.keyuan.demo.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.util.Arrays;
import java.util.Objects;

/**
 * @ClassName ElasticSearchRestConfig
 * @Description
 * @Author huangkeyuan
 * @Date 2020-08-12 16:57
 * @Version 1.0
 */
public class ElasticSearchRestConfig {
    @Value("${data.elasticsearch.url}")
    private String[] ipAddress;
    private static final int ADDRESS_LENGTH = 2;
    private static final String HTTP_SCHEME = "http";

    @Bean
    public RestClientBuilder restClientBuilder() {
        HttpHost[] hosts = Arrays.stream(ipAddress)
                .map(this::makeHttpHost)
                .filter(Objects::nonNull)
                .toArray(HttpHost[]::new);
        return RestClient.builder(hosts);
    }

    @Bean
    @Primary
    public RestHighLevelClient highLevelClient(@Autowired RestClientBuilder restClientBuilder) {
        return new RestHighLevelClient(restClientBuilder);
    }

    private HttpHost makeHttpHost(String s) {
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
