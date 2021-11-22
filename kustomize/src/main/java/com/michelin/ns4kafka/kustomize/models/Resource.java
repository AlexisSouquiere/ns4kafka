package com.michelin.ns4kafka.kustomize.models;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Iterator;
import java.util.Map;

@Introspected
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Resource {

    private String apiVersion;
    private String kind;
    private ObjectMeta metadata;
    private Map<String,Object> spec;
}
