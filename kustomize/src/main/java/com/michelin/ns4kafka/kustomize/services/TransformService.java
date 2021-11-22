package com.michelin.ns4kafka.kustomize.services;

import com.michelin.ns4kafka.kustomize.models.*;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.math.NumberUtils;

import javax.inject.Singleton;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Singleton
public class TransformService {

    public void initEnvResources(String envName) {
        if (!GeneratedResources.resources.containsKey(envName)) {
            Map<GeneratedResources.Kind, List<Resource>> resources = new HashMap<>();
            resources.put(GeneratedResources.Kind.TOPICS, new ArrayList<>());
            resources.put(GeneratedResources.Kind.CONNECTORS, new ArrayList<>());
            resources.put(GeneratedResources.Kind.STREAMS, new ArrayList<>());
            GeneratedResources.resources.put(envName, resources);
        }
    }

    Resource getTopicsResources(Input input, SpecObject object, String envName) {
        String templateName = input.getSpec().getTemplate();
        if (object.getTemplate() != null)
            templateName = object.getTemplate();
        Map<String, Object> template = LoadedResources.topicTemplates.get(templateName).getSpec();
        Resource env = LoadedResources.environments.get(envName);
        Map<String, Object> envProperties = (Map<String, Object>) env.getSpec().get("topics");
        String topicName = env.getSpec().get("prefix") == null ? object.getName() : env.getSpec().get("prefix") + object.getName();
        Resource resource = new Resource();
        resource.setApiVersion("v1");
        resource.setKind("Topic");
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setCluster((String) env.getSpec().get("cluster"));
        objectMeta.setNamespace((String) env.getSpec().get("namespace"));
        objectMeta.setName(topicName);
        resource.setMetadata(objectMeta);
        Map<String, Object> specConfig = new HashMap<>();
        Map<String, Object> config = new HashMap<>();

        fulfillWithResource(config, specConfig, envProperties);
        fulfillWithResource(config, specConfig, template);

        if (object.getOverrides() != null) {
            config = getSpecConfig(object.getOverrides(), config);
        }

        specConfig.put("configs", config);
        resource.setSpec(specConfig);

        GeneratedResources.resources.get(envName).get(GeneratedResources.Kind.TOPICS).add(resource);
        return resource;
    }


    private void fulfillWithResource(Map<String, Object> config, Map<String, Object> specConfig, Map<String, Object> input) {

        if (input.get("configs")!=null)
            config = getSpecConfig((Map<String, Object>) input.get("configs"), config);
        if (input.get("overrides")!=null)
            config = getSpecConfig((Map<String, Object>) input.get("overrides"), config);
        if (input.get("partitions")!=null)
            specConfig.put("partitions", input.get("partitions"));
        if (input.get("replicationFactor")!=null)
            specConfig.put("replicationFactor", input.get("replicationFactor"));
    }

    private Map<String, Object> getSpecConfig(Map<String, Object> spec, Map<String, Object> config) {

        for(String k : spec.keySet()) {
            if (spec.get(k)!=null)
            config.put(k, spec.get(k));
        }
        return config;
    }

    public Resource getConnectorResource(Input input, SpecObject object, String envName) {
        String templateName = input.getSpec().getTemplate();
        if (object.getTemplate()!=null)
            templateName = object.getTemplate();
        Resource template = LoadedResources.connectTemplates.get(templateName);
        Map<String, Object> templateOverrides = (Map<String, Object>) template.getSpec().get("overrides");
        Resource env = LoadedResources.environments.get(envName);
        Map<String,Map<String, Object>> connectors = (Map<String, Map<String, Object>>) env.getSpec().get("connectors");
        Map<String, Object> connectorProperties = connectors.get("properties");
        String connectName = env.getSpec().get("prefix") == null ? object.getName() : env.getSpec().get("prefix") + object.getName();
        Resource resource = new Resource();
        resource.setApiVersion("v1");
        resource.setKind("Connector");
        ObjectMeta objectMeta = new ObjectMeta();
        objectMeta.setCluster((String) env.getSpec().get("cluster"));
        objectMeta.setNamespace((String) env.getSpec().get("namespace"));
        objectMeta.setName(connectName);
        resource.setMetadata(objectMeta);
        Map<String, Object> specConfig = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        Pattern pattern = Pattern.compile("(.*?)\\$\\{(.*?)\\}(.*?)");
        for(String k : templateOverrides.keySet()) {
            if (templateOverrides.get(k)==null || templateOverrides.get(k) instanceof Integer || templateOverrides.get(k) instanceof Boolean) {
                config.put(k, templateOverrides.get(k));
                continue;
            }
            String value = (String) templateOverrides.get(k);
            Matcher matcher = pattern.matcher(value);
            StringBuffer sb = new StringBuffer();
            while (matcher.find()) {
                String group = matcher.group(2);
                if (group.startsWith("env:")) {
                    group = group.substring(4);
                    System.out.println("group = " + group);
                    if (connectorProperties.get(group) != null)
                        matcher.appendReplacement(sb, connectorProperties.get(group).toString());
                    else
                        matcher.appendReplacement(sb, "");
                }
                else
                    matcher.appendReplacement(sb, object.getProperties().get(group));
            }
            matcher.appendTail(sb);
            if (NumberUtils.isParsable(sb.toString()))
                config.put(k,Integer.parseInt(sb.toString()));
            else
                config.put(k, sb.toString());
        }
        config.put("name", connectName);
        if (object.getOverrides()!=null)
            object.getOverrides().keySet().stream().forEach(k -> config.put(k, object.getOverrides().get(k)));
        specConfig.put("connectCluster", connectors.get("connectCluster"));
        specConfig.put("config", new TreeMap(config));
        resource.setSpec(specConfig);

        GeneratedResources.resources.get(envName).get(GeneratedResources.Kind.CONNECTORS).add(resource);
        return resource;
    }

}
