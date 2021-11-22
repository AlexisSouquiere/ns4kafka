package com.michelin.ns4kafka.kustomize.models;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Introspected
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SpecObject {

    private String name;
    private String template;
    private Map<String,String>  properties;
    private Map<String,Object>  overrides;

}
