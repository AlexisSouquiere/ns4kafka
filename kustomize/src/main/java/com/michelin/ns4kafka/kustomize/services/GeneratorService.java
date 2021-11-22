package com.michelin.ns4kafka.kustomize.services;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.michelin.ns4kafka.kustomize.models.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Singleton
public class GeneratorService {

    @Inject
    TransformService transformService;

    public void generateResources(List<Resource> inputs) {

        arrangeResources(inputs);
        generateTopics();
        generateConnectors();
        generateStreams();
    }

    private void arrangeResources(List<Resource> inputs) {

        ObjectMapper mapper = new ObjectMapper();
        inputs.stream().forEach(r -> {
            switch (r.getKind()) {
                case "Environment":
                    LoadedResources.environments.put(r.getMetadata().getName(), r);
                    break;
                case "ConnectorsConfig":
                    Input connectInput = mapper.convertValue(r, Input.class);
                    LoadedResources.connectConfigs.add(connectInput);
                    break;
                case "ConnectTemplate":
                    LoadedResources.connectTemplates.put(r.getMetadata().getName(), r);
                    break;
                case "TopicsConfig":
                    Input topicInput = mapper.convertValue(r, Input.class);
                    LoadedResources.topicConfigs.add(topicInput);
                    break;
                case "TopicTemplate":
                    LoadedResources.topicTemplates.put(r.getMetadata().getName(), r);
                    break;
                case "StreamsConfig":
                    Input streamInput = mapper.convertValue(r, Input.class);
                    LoadedResources.streamConfigs.add(streamInput);
                    break;
            }
        });
    }

    private List<Resource> generateTopics() {
        Stream<Resource> topics = LoadedResources.topicConfigs.stream().flatMap(
                input -> input.getSpec().getObjects().stream().flatMap(
                        object ->
                             input.getSpec().getEnvironment().stream()
                                    .peek(transformService::initEnvResources)
                                    .map(envName -> transformService.getTopicsResources(input, object, envName))
                )
        );
        return topics.collect(Collectors.toList());
    }

    private List<Resource> generateConnectors() {
        Stream<Resource> connectors = LoadedResources.connectConfigs.stream().flatMap(
                input -> input.getSpec().getObjects().stream().flatMap(
                        object -> input.getSpec().getEnvironment().stream()
                                .peek(transformService::initEnvResources)
                                .map(envName -> transformService.getConnectorResource(input, object, envName))
                )
        );
        return connectors.collect(Collectors.toList());
    }

    private List<Resource> generateStreams() {
        Stream<Resource> streams =  LoadedResources.streamConfigs.stream().flatMap(
                input -> input.getSpec().getObjects().stream().flatMap(
                        object -> input.getSpec().getEnvironment().stream()
                                .peek(transformService::initEnvResources)
                                .map(
                                    envName -> {
                                        Resource env = LoadedResources.environments.get(envName);
                                        String streamName = env.getSpec().get("prefix") == null ? object.getName() : env.getSpec().get("prefix") + object.getName();

                                        Resource resource = new Resource();
                                        resource.setApiVersion("v1");
                                        resource.setKind("KafkaStream");
                                        ObjectMeta objectMeta = new ObjectMeta();
                                        objectMeta.setCluster((String) env.getSpec().get("cluster"));
                                        objectMeta.setNamespace((String) env.getSpec().get("namespace"));
                                        objectMeta.setName(streamName);
                                        resource.setMetadata(objectMeta);

                                        GeneratedResources.resources.get(envName).get(GeneratedResources.Kind.STREAMS).add(resource);
                                        return resource;
                                }
                        )
                ));
        return streams.collect(Collectors.toList());
    }

}
