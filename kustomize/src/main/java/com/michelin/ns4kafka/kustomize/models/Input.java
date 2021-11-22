package com.michelin.ns4kafka.kustomize.models;

import io.micronaut.core.annotation.Introspected;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Introspected
@Builder
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Input {

    private String apiVersion;
    private String kind;
    private ObjectMeta metadata;
    private Spec spec;
}
