package com.gooddata.dataload.processes;

import static com.gooddata.util.Validate.notEmpty;
import static com.gooddata.util.Validate.notNull;

import com.gooddata.gdc.ErrorStructure;
import com.gooddata.util.ISODateTimeDeserializer;
import com.gooddata.util.ISODateTimeSerializer;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.annotate.JsonTypeName;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.joda.time.DateTime;

import java.net.URI;
import java.util.Map;

/**
 * Dataload process execution detail. Deserialization only.
 */
@JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
@JsonTypeName("executionDetail")
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProcessExecutionDetail {

    private static final String LOG_LINK = "log";
    private static final String SELF_LINK = "self";
    private static final String EXECUTION_LINK = "poll";
    private static final String STATUS_OK = "OK";
    private final String status;

    private final DateTime created;
    private final DateTime started;
    private final DateTime updated;
    private final DateTime finished;

    private final ErrorStructure error;
    private final Map<String,String> links;

    @JsonCreator
    private ProcessExecutionDetail(@JsonProperty("status") String status,
                                   @JsonProperty("created") @JsonDeserialize(using = ISODateTimeDeserializer.class) DateTime created,
                                   @JsonProperty("started") @JsonDeserialize(using = ISODateTimeDeserializer.class) DateTime started,
                                   @JsonProperty("updated") @JsonDeserialize(using = ISODateTimeDeserializer.class) DateTime updated,
                                   @JsonProperty("finished") @JsonDeserialize(using = ISODateTimeDeserializer.class) DateTime finished,
                                   @JsonProperty("error") ErrorStructure error,
            @JsonProperty("links") Map<String, String> links) {
        this.status = notEmpty(status, "status");
        this.created = notNull(created, "created");
        this.started = started;
        this.updated = updated;
        this.finished = finished;
        this.error = error;
        this.links = links;
    }

    public String getStatus() {
        return status;
    }

    @JsonSerialize(using = ISODateTimeSerializer.class)
    public DateTime getCreated() {
        return created;
    }

    @JsonSerialize(using = ISODateTimeSerializer.class)
    public DateTime getStarted() {
        return started;
    }

    @JsonSerialize(using = ISODateTimeSerializer.class)
    public DateTime getUpdated() {
        return updated;
    }

    @JsonSerialize(using = ISODateTimeSerializer.class)
    public DateTime getFinished() {
        return finished;
    }

    public ErrorStructure getError() {
        return error;
    }

    @JsonIgnore
    public String getLogLink() {
        return links != null ? links.get(LOG_LINK) : null;
    }

    @JsonIgnore
    public String getUri() {
        return links != null ? links.get(SELF_LINK) : null;
    }

    @JsonIgnore
    public String getExecutionLink() {
        return links != null ? links.get(EXECUTION_LINK) : null;
    }

    @JsonIgnore
    public boolean isSuccess() {
        return STATUS_OK.equals(status);
    }


    public static URI uriFromExecutionUri(URI executionUri) {
        return URI.create(executionUri.toString() + "/detail");
    }
}
