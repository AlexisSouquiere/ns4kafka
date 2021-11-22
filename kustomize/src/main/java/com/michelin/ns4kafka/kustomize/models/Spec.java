package com.michelin.ns4kafka.kustomize.models;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Introspected
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Spec {

    private String template;
    private List<String> environment;
    private List<SpecObject> objects;
}
