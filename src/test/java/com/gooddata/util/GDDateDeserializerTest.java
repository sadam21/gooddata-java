package com.gooddata.util;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class GDDateDeserializerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    public void testDeserialize() throws Exception {
        final String json = MAPPER.writeValueAsString(new GDDateClass(new LocalDate(2012, 3, 20)));

        final JsonNode node = MAPPER.readTree(json);
        assertThat(node.path("date").getTextValue(), is("2012-03-20"));

        final GDDateClass date = MAPPER.readValue(json, GDDateClass.class);
        assertThat(date.getDate(), is(new LocalDate(2012, 3, 20)));
    }


}
