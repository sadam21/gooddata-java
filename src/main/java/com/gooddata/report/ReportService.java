/*
 * Copyright (C) 2007-2014, GoodData(R) Corporation. All rights reserved.
 */
package com.gooddata.report;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gooddata.AbstractService;
import com.gooddata.FutureResult;
import com.gooddata.GoodDataException;
import com.gooddata.GoodDataRestException;
import com.gooddata.PollResult;
import com.gooddata.SimplePollHandler;
import com.gooddata.gdc.UriResponse;
import com.gooddata.md.report.Report;
import com.gooddata.md.report.ReportDefinition;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.springframework.web.util.UriTemplate;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Map;

import static com.gooddata.util.Validate.notNull;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;

/**
 * Service for report export
 */
public class ReportService extends AbstractService {

    public static final String EXPORTING_URI = "/gdc/exporter/executor";
    public static final String RAW_CSV_EXPORTING_URI = "/gdc/projects/{projectId}/execute/raw";

    public ReportService(final RestTemplate restTemplate) {
        super(restTemplate);
    }

    /**
     * Export the given report definition in the given format to the given output strream
     * @param reportDefinition report definition
     * @param format export format
     * @param output target
     * @return polling result
     * @throws NoDataReportException in case report contains no data
     * @throws ReportException on error
     */
    public FutureResult<Void> exportReport(final ReportDefinition reportDefinition, final ReportExportFormat format,
                                           final OutputStream output) {
        notNull(reportDefinition, "reportDefinition");
        final ReportRequest request = new ExecuteDefinition(reportDefinition.getUri());
        return exportReport(request, format, output);
    }

    public void exportReportRawCsv(final Report report, final OutputStream output) {
        final String uri = restTemplate.postForObject(RAW_CSV_EXPORTING_URI, new ExecuteReport(report.getUri()), UriResponse.class, "b27pwwvlz5fnvdpiy4w3q257t1yvbw18").getUri();
        try {
            //URL url = new URL(uri);
            //System.out.println("url: " + url);

            final String uriDecoded = URLDecoder.decode(uri, "UTF-8");
            final URI uriO = URI.create("https://zebroids.intgdc.com" + uri);
//            final URI uriOO = new URI(uri);

            System.out.println("raw: " + uri);
            System.out.println("uri: " + uriO);
            System.out.println("dec: " + uriDecoded);
            UriTemplate uriTemplateFull = new UriTemplate("/gdc/projects/{projectId}/execute/raw/{executeId}?q={parQ}&c={parC}");
            UriTemplate uriTemplate = new UriTemplate("/gdc/projects/{projectId}/execute/raw/{executeId}");

            Map<String, String> params = uriTemplateFull.match(uri);

            params.put("parQ", URLDecoder.decode(params.get("parQ"), "UTF-8"));
            params.put("parC", URLDecoder.decode(params.get("parC"), "UTF-8"));

            final URI uriXX = UriComponentsBuilder
                    .fromUriString(uriTemplate.expand(params).toString())
                    .queryParam("q", params.get("parQ"))
                    .queryParam("c", params.get("parC"))
                    .build()
                    .toUri();

//            System.out.println(params);

            restTemplate.execute(uriO, GET, noopRequestCallback, new OutputStreamResponseExtractor(output));


            //final URI uriDDD = UriComponentsBuilder.fromUriString(uri).build().toUri();

            //final String configuredPath = serverUri.getPath();
//            final String resolvedPath = (configuredPath == null) ? uri.getPath() : configuredPath + uri.getPath(); // GD-23442 - resolve path
//            final UriComponentsBuilder builder = UriComponentsBuilder.newInstance();
//            builder
//                    .uri(URI.create("https://zebroids.intgdc.com"))
//                    .replacePath("/gdc/projects/b27pwwvlz5fnvdpiy4w3q257t1yvbw18/execute/raw/5f6736081c5468a760b8b689bffce87e00000152eec84d910000004c")
//                    .query(uriO.getRawQuery())
//                    .fragment(uriO.getRawFragment());   // raw form means do not 'decode'
//
//            final URI absoluteUri = builder.build(true).toUri(); // encode = true means do not encode URI components

            //System.out.println("d e: " + URLEncoder.encode(uriDecoded, "UTF-8"));
            //System.out.println("e d: " + URLEncoder.encode(URLDecoder.decode(uriDecoded, "UTF-8"), "UTF-8"));

           // restTemplate.execute(uri, GET, noopRequestCallback, new OutputStreamResponseExtractor(output));
//            restTemplate.execute(uriDecoded, GET, noopRequestCallback, new OutputStreamResponseExtractor(output));
            //restTemplate.execute(uriO, GET, noopRequestCallback, new OutputStreamResponseExtractor(output));
//            restTemplate.execute(uriTemplate.toString(), GET, noopRequestCallback, new OutputStreamResponseExtractor(output), params);
//            restTemplate.execute(uriTemplate.expand(params), GET, noopRequestCallback, new OutputStreamResponseExtractor(output));
//            restTemplate.execute(absoluteUri, GET, noopRequestCallback, new OutputStreamResponseExtractor(output));

//            URL url = new URL("https://zebroids.intgdc.com" + uri);

        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Export the given report in the given format to the given output strream
     * @param report report
     * @param format export format
     * @param output target
     * @return polling result
     * @throws NoDataReportException in case report contains no data
     * @throws ReportException on error
     */
    public FutureResult<Void> exportReport(final Report report, final ReportExportFormat format,
                                           final OutputStream output) {
        notNull(report, "report");
        final ReportRequest request = new ExecuteReport(report.getUri());
        return exportReport(request, format, output);
    }

    private FutureResult<Void> exportReport(final ReportRequest request, final ReportExportFormat format, final OutputStream output) {
        notNull(output, "output");
        notNull(format, "format");
        final JsonNode execResult = executeReport(request);
        final String uri = exportReport(execResult, format);
        return new PollResult<>(this, new SimplePollHandler<Void>(uri, Void.class) {
            @Override
            public boolean isFinished(ClientHttpResponse response) throws IOException {
                switch (response.getStatusCode()) {
                    case OK: return true;
                    case ACCEPTED: return false;
                    case NO_CONTENT: throw new NoDataReportException();
                    default: throw new ReportException("Unable to export report, unknown HTTP response code: " + response.getStatusCode());
                }
            }

            @Override
            public void handlePollException(final GoodDataRestException e) {
                throw new ReportException("Unable to export report", e);
            }

            @Override
            protected void onFinish() {
                try {
                    restTemplate.execute(uri, GET, noopRequestCallback, new OutputStreamResponseExtractor(output));
                } catch (GoodDataException | RestClientException e) {
                    throw new ReportException("Unable to export report", e);
                }
            }
        });
    }

    private JsonNode executeReport(final ReportRequest request) {
        try {
            final ResponseEntity<String> entity = restTemplate
                    .exchange(ReportRequest.URI, POST, new HttpEntity<>(request), String.class);
            return mapper.readTree(entity.getBody());
        } catch (GoodDataException | RestClientException e) {
            throw new ReportException("Unable to execute report", e);
        } catch (IOException e) {
            throw new ReportException("Unable to read execution result", e);
        }
    }

    private String exportReport(final JsonNode execResult, final ReportExportFormat format) {
        notNull(execResult, "execResult");
        notNull(format, "format");
        final ObjectNode root = mapper.createObjectNode();
        final ObjectNode child = mapper.createObjectNode();

        try {
            child.put("result", execResult);
            child.put("format", format.getValue());
            root.put("result_req", child);
            return restTemplate.postForObject(EXPORTING_URI, root, UriResponse.class).getUri();
        } catch (GoodDataException | RestClientException e) {
            throw new ReportException("Unable to export report", e);
        }
    }
}
