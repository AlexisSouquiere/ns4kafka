package com.michelin.ns4kafka.kustomize.models;

import io.micronaut.core.annotation.Introspected;
import lombok.*;

import java.util.Map;

@Introspected
@Getter
@Setter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ObjectMeta {
	private String name;
	private String namespace;
	private String cluster;
	private Map<String,String> labels;
}
