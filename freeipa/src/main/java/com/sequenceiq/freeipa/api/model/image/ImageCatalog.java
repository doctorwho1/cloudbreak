package com.sequenceiq.freeipa.api.model.image;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ImageCatalog {

    private final Images images;

    @JsonCreator
    public ImageCatalog(@JsonProperty(value = "images", required = true) Images images) {
        this.images = images;
    }

    public Images getImages() {
        return images;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ImageCatalog{");
        sb.append("images=").append(images);
        sb.append('}');
        return sb.toString();
    }
}
