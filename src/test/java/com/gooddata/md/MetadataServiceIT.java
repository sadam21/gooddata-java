/*
 * Copyright (C) 2007-2014, GoodData(R) Corporation. All rights reserved.
 */
package com.gooddata.md;

import com.gooddata.AbstractGoodDataIT;
import com.gooddata.gdc.UriResponse;
import com.gooddata.project.Project;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;

import static net.jadler.Jadler.onRequest;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

public class MetadataServiceIT extends AbstractGoodDataIT {

    private static final String OBJ_URI = "/gdc/md/PROJECT_ID/obj";
    private static final String SPECIFIC_OBJ_URI = "/gdc/md/PROJECT_ID/obj/17";

    private Project project;
    private Metric metricInput;

    @BeforeMethod
    public void setUp() throws Exception {
        project = MAPPER.readValue(readResource("/project/project.json"), Project.class);
        metricInput = MAPPER.readValue(readResource("/md/metric-input.json"), Metric.class);
    }

    @Test
    public void shouldCreateObj() throws Exception {
        onRequest()
                .havingMethodEqualTo("POST")
                .havingPathEqualTo(OBJ_URI)
            .respond()
                .withStatus(200)
                .withBody(MAPPER.writeValueAsString(new UriResponse(SPECIFIC_OBJ_URI)));
        onRequest()
                .havingMethodEqualTo("GET")
                .havingPathEqualTo(SPECIFIC_OBJ_URI)
            .respond()
                .withStatus(200)
                .withBody(readResource("/md/metric.json"));

        final Obj result = gd.getMetadataService().createObj(project, metricInput);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(instanceOf(Metric.class)));
        assertThat(((Metric) result).getTitle(), is("Person Name"));
        assertThat(((Metric) result).getFormat(), is("FORMAT"));
    }

    @Test
    public void shouldGetObjByUri() throws Exception {
        onRequest()
                .havingMethodEqualTo("GET")
                .havingPathEqualTo(SPECIFIC_OBJ_URI)
            .respond()
                .withStatus(200)
                .withBody(readResource("/md/metric.json"));

        final Obj result = gd.getMetadataService().getObjByUri(SPECIFIC_OBJ_URI, Metric.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(instanceOf(Metric.class)));
        assertThat(((Metric) result).getTitle(), is("Person Name"));
        assertThat(((Metric) result).getFormat(), is("FORMAT"));
    }

    @Test
    public void shouldGetObjById() throws Exception {
        onRequest()
                .havingMethodEqualTo("GET")
                .havingPathEqualTo(SPECIFIC_OBJ_URI)
            .respond()
                .withStatus(200)
                .withBody(readResource("/md/metric.json"));

        final Obj result = gd.getMetadataService().getObjById(project, "17", Metric.class);

        assertThat(result, is(notNullValue()));
        assertThat(result, is(instanceOf(Metric.class)));
        assertThat(result.getUri(), is("/gdc/md/PROJECT_ID/obj/DF_ID"));
        assertThat(((Metric) result).getTitle(), is("Person Name"));
        assertThat(((Metric) result).getFormat(), is("FORMAT"));
    }

    @Test
    public void shouldGetObjUriByRestrictions() throws Exception {
        onRequest()
                .havingMethodEqualTo("GET")
                .havingPathEqualTo("/gdc/md/PROJECT_ID/query/attributes")
            .respond()
                .withStatus(200)
                .withBody(readResource("/md/query.json"));

        final String result = gd.getMetadataService().getObjUri(project, Attribute.class, Restriction.title("Resource"));

        assertThat(result, is(notNullValue()));
        assertThat(result, is("/gdc/md/PROJ_ID/obj/127"));
    }

    @Test
    public void shouldGetObjByRestrictions() throws Exception {
        onRequest()
                .havingMethodEqualTo("GET")
                .havingPathEqualTo("/gdc/md/PROJECT_ID/query/attributes")
            .respond()
                .withStatus(200)
                .withBody(readResource("/md/query.json"));
        onRequest()
                .havingMethodEqualTo("GET")
                .havingPathEqualTo("/gdc/md/PROJ_ID/obj/118")
            .respond()
                .withStatus(200)
                .withBody(readResource("/md/attribute.json"));

        final Obj result = gd.getMetadataService().getObj(project, Attribute.class, Restriction.title("Name"));

        assertThat(result, is(notNullValue()));
        assertThat(result, is(instanceOf(Attribute.class)));
        assertThat(result.getUri(), is("/gdc/md/PROJECT_ID/obj/28"));
        assertThat(((Attribute) result).getTitle(), is("Person ID"));
    }

    @Test
    public void shouldFindByRestrictions() throws Exception {
        onRequest()
                .havingMethodEqualTo("GET")
                .havingPathEqualTo("/gdc/md/PROJECT_ID/query/attributes")
            .respond()
                .withStatus(200)
                .withBody(readResource("/md/query.json"));

        final Collection<Entry> result = gd.getMetadataService()
                .find(project, Attribute.class, Restriction.summary(""));

        assertThat(result, is(notNullValue()));
        assertThat(result, hasSize(2));
    }

    @Test
    public void shouldFindUrisByRestrictions() throws Exception {
        onRequest()
                .havingMethodEqualTo("GET")
                .havingPathEqualTo("/gdc/md/PROJECT_ID/query/attributes")
            .respond()
                .withStatus(200)
                .withBody(readResource("/md/query.json"));

        final Collection<String> result = gd.getMetadataService()
                .findUris(project, Attribute.class, Restriction.summary(""));

        assertThat(result, is(notNullValue()));
        assertThat(result, hasSize(2));
        assertThat(result, contains("/gdc/md/PROJ_ID/obj/127", "/gdc/md/PROJ_ID/obj/118"));
    }
}