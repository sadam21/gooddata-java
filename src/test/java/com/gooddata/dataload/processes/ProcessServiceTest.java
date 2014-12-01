/*
 * Copyright (C) 2007-2014, GoodData(R) Corporation. All rights reserved.
 */
package com.gooddata.dataload.processes;

import com.gooddata.GoodDataException;
import com.gooddata.GoodDataRestException;
import com.gooddata.account.Account;
import com.gooddata.account.AccountService;
import com.gooddata.project.Project;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

import static java.net.URI.create;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

public class ProcessServiceTest {

    private static final String PROCESS_ID = "11";
    private static final String PROJECT_ID = "17";
    private static final String ACCOUNT_ID = "7";
    private static final String URI = "/gdc/projects/17/dataload/processes/11";
    private static final String USER_PROCESS_URI = "/gdc/account/profile/7/dataload/processes";

    @Mock
    private Project project;
    @Mock
    private DataloadProcess process;
    @Mock
    private RestTemplate restTemplate;
    @Mock
    private AccountService accountService;
    @Mock
    private Account account;

    private ProcessService service;

    @BeforeMethod
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        service = new ProcessService(restTemplate, accountService);
        when(process.getId()).thenReturn(PROCESS_ID);
        when(project.getId()).thenReturn(PROJECT_ID);
        when(accountService.getCurrent()).thenReturn(account);
        when(account.getId()).thenReturn(ACCOUNT_ID);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUpdateProcessWithNullProject() throws Exception {
        service.updateProcess(null, process, File.createTempFile("test", null));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUpdateProcessWithNullProcess() throws Exception {
        service.updateProcess(project, null, File.createTempFile("test", null));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testUpdateProcessWithNullFile() throws Exception {
        service.updateProcess(project, process, null);
    }

    @Test(expectedExceptions = GoodDataException.class)
    public void testUpdateProcessWithRestClientError() throws Exception {
        when(restTemplate.postForObject(eq(create(URI)), any(), eq(DataloadProcess.class)))
                .thenThrow(new RestClientException(""));
        service.updateProcess(project, process, File.createTempFile("test", null));
    }

    @Test
    public void testUpdateProcess() throws Exception {
        when(restTemplate.postForObject(eq(create(URI)), any(), eq(DataloadProcess.class))).thenReturn(process);
        final DataloadProcess result = service.updateProcess(project, process, File.createTempFile("test", null));
        assertThat(result, is(process));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetProcessByUriWithNullUri() throws Exception {
        service.getProcessByUri(null);
    }

    @Test
    public void testGetProcessByUri() throws Exception {
        when(restTemplate.getForObject(URI, DataloadProcess.class)).thenReturn(process);

        final DataloadProcess result = service.getProcessByUri(URI);
        assertThat(result, is(process));
    }

    @Test(expectedExceptions = ProcessNotFoundException.class)
    public void testGetProcessByUriNotFound() throws Exception {
        when(restTemplate.getForObject(URI, DataloadProcess.class)).thenThrow(
                new GoodDataRestException(404, "", "", "", ""));
        service.getProcessByUri(URI);
    }

    @Test(expectedExceptions = GoodDataRestException.class)
    public void testGetProcessByUriServerError() throws Exception {
        when(restTemplate.getForObject(URI, DataloadProcess.class))
                .thenThrow(new GoodDataRestException(500, "", "", "", ""));
        service.getProcessByUri(URI);
    }

    @Test(expectedExceptions = GoodDataException.class)
    public void testGetProcessByUriClientError() throws Exception {
        when(restTemplate.getForObject(URI, DataloadProcess.class)).thenThrow(new RestClientException(""));
        service.getProcessByUri(URI);
    }

    @Test(expectedExceptions = GoodDataException.class)
    public void testListUserProcessesWithRestClientError() throws Exception {
        when(restTemplate.getForObject(create(USER_PROCESS_URI), DataloadProcesses.class))
                .thenThrow(new RestClientException(""));
        service.listUserProcesses();
    }

    @Test(expectedExceptions = GoodDataException.class)
    public void testListUserProcessesWithNullResponse() throws Exception {
        when(restTemplate.getForObject(create(USER_PROCESS_URI), DataloadProcesses.class)).thenReturn(null);
        service.listUserProcesses();
    }

    @Test
    public void testListUserProcessesWithNoProcesses() throws Exception {
        when(restTemplate.getForObject(create(USER_PROCESS_URI), DataloadProcesses.class))
                .thenReturn(new DataloadProcesses(Collections.<DataloadProcess>emptyList()));
        final Collection<DataloadProcess> result = service.listUserProcesses();
        assertThat(result, empty());
    }

    @Test
    public void testListUserProcessesWithOneProcesses() throws Exception {
        when(restTemplate.getForObject(create(USER_PROCESS_URI), DataloadProcesses.class))
                .thenReturn(new DataloadProcesses(asList(process)));
        final Collection<DataloadProcess> result = service.listUserProcesses();
        assertThat(result, hasSize(1));
        assertThat(result, contains(process));
    }
}