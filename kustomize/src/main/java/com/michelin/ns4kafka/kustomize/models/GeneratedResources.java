package com.michelin.ns4kafka.kustomize.models;

import com.michelin.ns4kafka.kustomize.models.Resource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeneratedResources {

    public enum Kind {
        TOPICS,
        STREAMS,
        CONNECTORS
    }

    public static Map<String, Map<Kind,List<Resource>>> resources = new HashMap<>();
}
