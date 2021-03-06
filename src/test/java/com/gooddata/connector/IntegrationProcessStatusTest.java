/*
 * Copyright (C) 2007-2014, GoodData(R) Corporation. All rights reserved.
 */
package com.gooddata.connector;

import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static com.gooddata.connector.Status.Code.ERROR;
import static com.gooddata.connector.Status.Code.SYNCHRONIZED;
import static com.gooddata.connector.Status.Code.UPLOADING;
import static com.gooddata.connector.Status.Code.USER_ERROR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.joda.time.DateTime.now;

public class IntegrationProcessStatusTest {

    @Test
    public void testShouldDeserialize() throws Exception {
        final IntegrationProcessStatus process = new ObjectMapper()
                .readValue(getClass().getResource("/connector/process-status-embedded.json"), IntegrationProcessStatus.class);

        assertThat(process, is(notNullValue()));
        assertThat(process.getStarted(), is(new DateTime(2014, 5, 30, 7, 50, 15, DateTimeZone.UTC)));
        assertThat(process.getFinished(), is(new DateTime(2014, 5, 30, 7, 50, 50, DateTimeZone.UTC)));
        assertThat(process.getStatus(), is(notNullValue()));
        assertThat(process.getStatus().getCode(), is(ERROR.name()));
        assertThat(process.getStatus().getDetail(), is("GDC-INTERNAL-ERROR"));
        assertThat(process.getStatus().getDescription(), is(nullValue()));
    }

    @Test
    public void testIsFinishedOnError() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status(ERROR.name(), "", ""), now(), now());
        assertThat(process.isFinished(), is(true));
    }

    @Test
    public void testIsFinishedOnSynchronized() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status(SYNCHRONIZED.name(), "", ""), now(), now());
        assertThat(process.isFinished(), is(true));
    }

    @Test
    public void testIsFinishedOnUploading() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status(UPLOADING.name(), "", ""), now(), now());
        assertThat(process.isFinished(), is(false));
    }

    @Test
    public void testIsFinishedOnNullCode() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status(null, "", ""), now(), now());
        assertThat(process.isFinished(), is(false));
    }

    @Test
    public void testIsFinishedOnUnknownCode() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status("unknown code", "", ""), now(), now());
        assertThat(process.isFinished(), is(false));
    }

    @Test
    public void testIsFailedOnError() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status(ERROR.name(), "", ""), now(), now());
        assertThat(process.isFailed(), is(true));
    }

    @Test
    public void testIsFailedOnUserError() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status(USER_ERROR.name(), "", ""), now(), now());
        assertThat(process.isFailed(), is(true));
    }

    @Test
    public void testIsFailedOnSynchronized() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status(SYNCHRONIZED.name(), "", ""), now(), now());
        assertThat(process.isFailed(), is(false));
    }

    @Test
    public void testIsFailedOnNullCode() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status(null, "", ""), now(), now());
        assertThat(process.isFailed(), is(false));
    }

    @Test
    public void testIsFailedOnUnknownCode() throws Exception {
        final IntegrationProcessStatus process = new IntegrationProcessStatus(new Status("unknown code", "", ""), now(), now());
        assertThat(process.isFailed(), is(false));
    }

}
