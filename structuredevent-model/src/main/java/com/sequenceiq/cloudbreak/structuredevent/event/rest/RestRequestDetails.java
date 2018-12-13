package com.sequenceiq.cloudbreak.structuredevent.event.rest;

import static com.sequenceiq.cloudbreak.structuredevent.event.rest.RestCallDetails.KAFKA_PROPERTY_FILTER_NAME;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.sequenceiq.cloudbreak.structuredevent.json.AnonymizingBase64Serializer;
import com.sequenceiq.cloudbreak.structuredevent.json.Base64Deserializer;

@JsonFilter(KAFKA_PROPERTY_FILTER_NAME)
public class RestRequestDetails implements Serializable {
    private String requestUri;

    private String mediaType;

    private String method;

    private Map<String, String> headers;

    private Map<String, String> cookies;

    @JsonSerialize(using = AnonymizingBase64Serializer.class)
    @JsonDeserialize(using = Base64Deserializer.class)
    private String body;

    public String getRequestUri() {
        return requestUri;
    }

    public void setRequestUri(String requestUri) {
        this.requestUri = requestUri;
    }

    public String getMediaType() {
        return mediaType;
    }

    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public Map<String, String> getCookies() {
        return cookies;
    }

    public void setCookies(Map<String, String> cookies) {
        this.cookies = cookies;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}
