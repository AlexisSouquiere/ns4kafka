package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.connector.Connector;
import com.michelin.ns4kafka.repositories.ConnectorRepository;
import com.michelin.ns4kafka.services.clients.connect.KafkaConnectClient;
import com.michelin.ns4kafka.services.clients.connect.entities.ConnectorSpecs;
import com.michelin.ns4kafka.services.executors.ConnectorAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.core.util.StringUtils;
import io.micronaut.http.HttpResponse;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.michelin.ns4kafka.utils.config.ConnectorConfig.CONNECTOR_CLASS;

@Slf4j
@Singleton
public class ConnectorService {
    @Inject
    AccessControlEntryService accessControlEntryService;

    @Inject
    KafkaConnectClient kafkaConnectClient;

    @Inject
    ConnectorRepository connectorRepository;

    @Inject
    ApplicationContext applicationContext;

    @Inject
    ConnectClusterService connectClusterService;

    /**
     * Find all connectors by given namespace
     * @param namespace The namespace
     * @return A list of connectors
     */
    public List<Connector> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace);
        return connectorRepository.findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(connector -> acls.stream().anyMatch(accessControlEntry -> {
                    if (accessControlEntry.getSpec().getPermission() != AccessControlEntry.Permission.OWNER) {
                        return false;
                    }
                    if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.CONNECT) {
                        switch (accessControlEntry.getSpec().getResourcePatternType()) {
                            case PREFIXED:
                                return connector.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                            case LITERAL:
                                return connector.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                        }
                    }
                    return false;
                }))
                .toList();
    }

    /**
     * Find all connectors by given namespace and Connect cluster
     * @param namespace The namespace
     * @param connectCluster The Connect cluster
     * @return A list of connectors
     */
    public List<Connector> findAllByConnectCluster(Namespace namespace, String connectCluster) {
        return connectorRepository.findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(connector -> connector.getSpec().getConnectCluster().equals(connectCluster))
                .toList();
    }

    /**
     * Find a connector by namespace and name
     * @param namespace The namespace
     * @param connector The connector name
     * @return An optional connector
     */
    public Optional<Connector> findByName(Namespace namespace, String connector) {
        return findAllForNamespace(namespace)
                .stream()
                .filter(connect -> connect.getMetadata().getName().equals(connector))
                .findFirst();
    }

    /**
     * Validate configurations of a given connector against the namespace rules
     * @param namespace The namespace
     * @param connector The connector to validate
     * @return A list of errors
     */
    public Mono<List<String>> validateLocally(Namespace namespace, Connector connector) {
        // Check whether target Connect Cluster is allowed for this namespace
        List<String> selfDeployedConnectClusters = connectClusterService.findAllByNamespaceWrite(namespace)
                .stream()
                .map(connectCluster -> connectCluster.getMetadata().getName()).toList();

        if (!namespace.getSpec().getConnectClusters().contains(connector.getSpec().getConnectCluster()) &&
                !selfDeployedConnectClusters.contains(connector.getSpec().getConnectCluster())) {
            String allowedConnectClusters = Stream.concat(namespace.getSpec().getConnectClusters().stream(), selfDeployedConnectClusters.stream()).collect(Collectors.joining(", "));
            return Mono.just(
                    List.of("Invalid value " + connector.getSpec().getConnectCluster() + " for spec.connectCluster: Value must be one of [" + allowedConnectClusters + "]"));
        }

        // If class doesn't exist, no need to go further
        if (StringUtils.isEmpty(connector.getSpec().getConfig().get(CONNECTOR_CLASS))) {
            return Mono.just(List.of("Invalid value for spec.config.'connector.class': Value must be non-null"));
        }

        // Connector type exists on this target connect cluster ?
        return kafkaConnectClient.connectPlugins(namespace.getMetadata().getCluster(), connector.getSpec().getConnectCluster())
                .map(connectorPluginInfos -> {
                    Optional<String> connectorType = connectorPluginInfos
                            .stream()
                            .filter(connectPluginItem -> connectPluginItem.className().equals(connector.getSpec().getConfig().get(CONNECTOR_CLASS)))
                            .map(connectorPluginInfo -> connectorPluginInfo.type().toString().toLowerCase(Locale.ROOT))
                            .findFirst();

                    if (connectorType.isEmpty()) {
                        return List.of("Failed to find any class that implements Connector and which name matches " +
                                connector.getSpec().getConfig().get(CONNECTOR_CLASS));
                    }

                    return namespace.getSpec().getConnectValidator() != null ? namespace.getSpec().getConnectValidator().validate(connector, connectorType.get())
                            : Collections.emptyList();
                });
    }

    /**
     * Is given namespace owner of the given connector
     * @param namespace The namespace
     * @param connect The connector
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfConnect(Namespace namespace, String connect) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace.getMetadata().getName(), AccessControlEntry.ResourceType.CONNECT, connect);
    }

    /**
     * Validate configurations of a given connector against the cluster
     * @param namespace The namespace
     * @param connector The connector
     * @return A list of errors
     */
    public Mono<List<String>> validateRemotely(Namespace namespace, Connector connector) {
        return kafkaConnectClient.validate(namespace.getMetadata().getCluster(),
                                connector.getSpec().getConnectCluster(), connector.getSpec().getConfig().get(CONNECTOR_CLASS),
                        ConnectorSpecs.builder().config(connector.getSpec().getConfig()).build())
                .map(configInfos -> configInfos.configs()
                        .stream()
                        .filter(configInfo -> !configInfo.configValue().errors().isEmpty())
                        .flatMap(configInfo -> configInfo.configValue().errors().stream())
                        .toList());
    }

    /**
     * Create a given connector
     * @param connector The connector to create
     * @return The created connector
     */
    public Connector createOrUpdate(Connector connector) {
        return connectorRepository.create(connector);
    }

    /**
     * Delete a given connector
     * @param namespace The namespace
     * @param connector The connector
     */
    public Mono<HttpResponse<Void>> delete(Namespace namespace, Connector connector) {
        return kafkaConnectClient.delete(namespace.getMetadata().getCluster(), connector.getSpec().getConnectCluster(), connector.getMetadata().getName())
                .defaultIfEmpty(HttpResponse.noContent())
                .map(httpResponse -> {
                    connectorRepository.delete(connector);

                    if (log.isInfoEnabled()) {
                        log.info("Success removing Connector [" + connector.getMetadata().getName() +
                                "] on Kafka [" + namespace.getMetadata().getName() +
                                "] Connect [" + connector.getSpec().getConnectCluster() + "]");
                    }

                    return httpResponse;
                });
    }

    /**
     * List the connectors that are not synchronized to ns4kafka by namespace
     * @param namespace The namespace
     * @return The list of connectors
     */
    public Mono<List<Connector>> listUnsynchronizedConnectors(Namespace namespace) {
        ConnectorAsyncExecutor connectorAsyncExecutor = applicationContext.getBean(ConnectorAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        // Get all connectors from all connect clusters
        List<Flux<Connector>> connectors = Stream.concat(namespace.getSpec().getConnectClusters().stream(),
                connectClusterService.findAllByNamespaceWrite(namespace)
                        .stream()
                        .map(connectCluster -> connectCluster.getMetadata().getName()))
                .map(connectClusterName -> connectorAsyncExecutor.collectBrokerConnectors(connectClusterName)
                        .flatMapIterable(brokerConnectors -> brokerConnectors)
                        // That belongs to this namespace
                        .filter(connector -> isNamespaceOwnerOfConnect(namespace, connector.getMetadata().getName()))
                        // And aren't in ns4kafka storage
                        .filter(connector -> findByName(namespace, connector.getMetadata().getName()).isEmpty()))
                .toList();

        return Flux.concat(connectors).collectList();
    }

    /**
     * Restart a given connector
     * @param namespace The namespace
     * @param connector The connector
     * @return An HTTP response
     */
    public Mono<HttpResponse<Void>> restart(Namespace namespace, Connector connector) {
        return kafkaConnectClient.status(namespace.getMetadata().getCluster(), connector.getSpec().getConnectCluster(), connector.getMetadata().getName())
                .flatMap(status -> {
                            Flux<HttpResponse<Void>> responses = Flux.fromIterable(status.tasks())
                                    .flatMap(task -> kafkaConnectClient.restart(namespace.getMetadata().getCluster(),
                                            connector.getSpec().getConnectCluster(), connector.getMetadata().getName(), task.getId()))
                                    .map(response -> {
                                        log.info("Success restarting connector [{}] on namespace [{}] connect [{}]",
                                                connector.getMetadata().getName(),
                                                namespace.getMetadata().getName(),
                                                connector.getSpec().getConnectCluster());
                                        return HttpResponse.ok();
                                    });

                            return Mono.from(responses);
                        });
    }

    /**
     * Pause a given connector
     * @param namespace The namespace
     * @param connector The connector
     * @return An HTTP response
     */
    public Mono<HttpResponse<Void>> pause(Namespace namespace, Connector connector) {
        return kafkaConnectClient.pause(namespace.getMetadata().getCluster(),
                connector.getSpec().getConnectCluster(), connector.getMetadata().getName())
                        .map(pause -> {
                            log.info("Success pausing Connector [{}] on Namespace [{}] Connect [{}]",
                                    connector.getMetadata().getName(),
                                    namespace.getMetadata().getName(),
                                    connector.getSpec().getConnectCluster());

                            return HttpResponse.accepted();
                        });
    }

    /**
     * Resume a given connector
     * @param namespace The namespace
     * @param connector The connector
     * @return An HTTP response
     */
    public Mono<HttpResponse<Void>> resume(Namespace namespace, Connector connector) {
        return kafkaConnectClient.resume(namespace.getMetadata().getCluster(),
                connector.getSpec().getConnectCluster(), connector.getMetadata().getName())
                        .map(resume -> {
                            log.info("Success resuming Connector [{}] on Namespace [{}] Connect [{}]",
                                    connector.getMetadata().getName(),
                                    namespace.getMetadata().getName(),
                                    connector.getSpec().getConnectCluster());

                            return HttpResponse.accepted();
                        });
    }
}
