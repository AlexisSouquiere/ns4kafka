package com.michelin.ns4kafka.kustomize.models;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadedResources {

    public static Map<String, Resource> environments = new HashMap<>();

    public static List<Input> connectConfigs = new ArrayList<>();
    public static Map<String, Resource> connectTemplates = new HashMap<>();

    public static List<Input> topicConfigs = new ArrayList<>();
    public static Map<String, Resource> topicTemplates = new HashMap<>();

    public static List<Input> streamConfigs = new ArrayList<>();
}
