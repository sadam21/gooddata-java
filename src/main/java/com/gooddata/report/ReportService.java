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
import com.gooddata.md.Obj;
import com.gooddata.md.report.Report;
import com.gooddata.md.report.ReportDefinition;
import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.util.FileCopyUtils;
import org.springframework.web.client.ResponseExtractor;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

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
     * Export the given report definition in the given format to the given output stream
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

    public void exportReportRawCsvHost(final Report report, final OutputStream output) {
        final String projectId = Obj.OBJ_TEMPLATE.match(report.getUri()).get("projectId");
        final String taskUri = restTemplate.postForObject(RAW_CSV_EXPORTING_URI, new ExecuteReport(report.getUri()), UriResponse.class, projectId).getUri();
        final String baseUrl = ((DefaultUriTemplateHandler) restTemplate.getUriTemplateHandler()).getBaseUrl();
        final URI uri = URI.create(baseUrl + taskUri);
        System.out.println(uri);
        while (!restTemplate.execute(uri, GET, noopRequestCallback, new ResponseExtractor<Boolean>() {

            @Override
            public Boolean extractData(final ClientHttpResponse response) throws IOException {
                final String contentType = response.getHeaders().get("Content-Type").get(0);
                System.out.println("Content-Type:" + contentType);
                if (contentType.startsWith("application/json")) {
                    response.close();
                    return false;
                } else if (contentType.startsWith("text/csv")) {
                    FileCopyUtils.copy(response.getBody(), output);
                    return true;
                } else {
                    throw new ReportException("Unable to export report.");
                }
            }

        })) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
