/*
 * Copyright (C) 2007-2019, GoodData(R) Corporation. All rights reserved.
 */

package com.gooddata;

import com.gooddata.gdc.Header;
import com.gooddata.util.ResponseErrorHandler;
import org.apache.http.client.HttpClient;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.StreamUtils;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static com.gooddata.util.Validate.notNull;
import static java.util.Arrays.asList;

public class GoodDataRestTemplateFactory {

    public RestTemplate create(GoodDataEndpoint endpoint, HttpClient httpClient) {
        notNull(endpoint, "endpoint");
        notNull(httpClient, "httpClient");

        final UriPrefixingClientHttpRequestFactory factory = new UriPrefixingClientHttpRequestFactory(
                new HttpComponentsClientHttpRequestFactory(httpClient),
                endpoint.toUri()
        );

        final Map<String, String> presetHeaders = new HashMap<>(2);
        presetHeaders.put("Accept", MediaType.APPLICATION_JSON_VALUE);
        presetHeaders.put(Header.GDC_VERSION, readApiVersion());

        final RestTemplate restTemplate = new RestTemplate(factory);
        restTemplate.setInterceptors(asList(
                new HeaderSettingRequestInterceptor(presetHeaders),
                new DeprecationWarningRequestInterceptor()));

        restTemplate.setErrorHandler(new ResponseErrorHandler(restTemplate.getMessageConverters()));

        return restTemplate;
    }

    private static String readApiVersion() {
        try {
            return StreamUtils.copyToString(GoodData.class.getResourceAsStream("/GoodDataApiVersion"), Charset.defaultCharset());
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read GoodDataApiVersion from classpath", e);
        }
    }
}
