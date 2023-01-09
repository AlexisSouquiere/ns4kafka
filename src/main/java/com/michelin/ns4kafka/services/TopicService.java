package com.michelin.ns4kafka.services;

import com.michelin.ns4kafka.models.AccessControlEntry;
import com.michelin.ns4kafka.models.Namespace;
import com.michelin.ns4kafka.models.Topic;
import com.michelin.ns4kafka.repositories.TopicRepository;
import com.michelin.ns4kafka.config.KafkaAsyncExecutorConfig;
import com.michelin.ns4kafka.services.executors.TopicAsyncExecutor;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.kafka.common.config.TopicConfig.*;

@Singleton
public class TopicService {
    /**
     * The topic repository
     */
    @Inject
    TopicRepository topicRepository;

    /**
     * The ACL service
     */
    @Inject
    AccessControlEntryService accessControlEntryService;

    /**
     * The application context
     */
    @Inject
    ApplicationContext applicationContext;

    /**
     * The managed cluster config
     */
    @Inject
    List<KafkaAsyncExecutorConfig> kafkaAsyncExecutorConfig;

    /**
     * Find all topics
     * @return The list of topics
     */
    public List<Topic> findAll() {
        return topicRepository.findAll();
    }

    /**
     * Find all topics by given namespace
     * @param namespace The namespace
     * @return A list of topics
     */
    public List<Topic> findAllForNamespace(Namespace namespace) {
        List<AccessControlEntry> acls = accessControlEntryService.findAllGrantedToNamespace(namespace);
        return topicRepository.findAllForCluster(namespace.getMetadata().getCluster())
                .stream()
                .filter(topic -> acls.stream().anyMatch(accessControlEntry -> {
                    //need to check accessControlEntry.Permission, we want OWNER
                    if (accessControlEntry.getSpec().getPermission() != AccessControlEntry.Permission.OWNER) {
                        return false;
                    }
                    if (accessControlEntry.getSpec().getResourceType() == AccessControlEntry.ResourceType.TOPIC) {
                        switch (accessControlEntry.getSpec().getResourcePatternType()) {
                            case PREFIXED:
                                return topic.getMetadata().getName().startsWith(accessControlEntry.getSpec().getResource());
                            case LITERAL:
                                return topic.getMetadata().getName().equals(accessControlEntry.getSpec().getResource());
                        }
                    }
                    return false;
                }))
                .collect(Collectors.toList());
    }

    /**
     * Find a topic by namespace and name
     * @param namespace The namespace
     * @param topic The topic name
     * @return An optional topic
     */
    public Optional<Topic> findByName(Namespace namespace, String topic) {
        return findAllForNamespace(namespace)
                .stream()
                .filter(t -> t.getMetadata().getName().equals(topic))
                .findFirst();
    }

    /**
     * Is given namespace owner of the given topic
     * @param namespace The namespace
     * @param topic The topic
     * @return true if it is, false otherwise
     */
    public boolean isNamespaceOwnerOfTopic(String namespace, String topic) {
        return accessControlEntryService.isNamespaceOwnerOfResource(namespace, AccessControlEntry.ResourceType.TOPIC, topic);
    }

    /**
     * Create a given topic
     * @param topic The topic to create
     * @return The created topic
     */
    public Topic create(Topic topic) {
        return topicRepository.create(topic);
    }

    /**
     * Delete a given topic
     * @param topic The topic
     */
    public void delete(Topic topic) throws InterruptedException, ExecutionException, TimeoutException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(topic.getMetadata().getCluster()));
        topicAsyncExecutor.deleteTopic(topic);

        topicRepository.delete(topic);
    }

    /**
     * List all topics colliding with existing topics on broker but not in ns4kafka
     * @param namespace The namespace
     * @param topic The topic
     * @return The list of colliding topics
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     * @throws TimeoutException Any timeout exception
     */
    public List<String> findCollidingTopics(Namespace namespace, Topic topic) throws InterruptedException, ExecutionException, TimeoutException  {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        try {
            List<String> clusterTopics = topicAsyncExecutor.listBrokerTopicNames();
            return clusterTopics.stream()
                    // existing topics with the exact same name (and not currently in ns4kafka) should not interfere
                    // this topic could be created on ns4kafka during "import" step
                    .filter(clusterTopic -> !topic.getMetadata().getName().equals(clusterTopic))
                    .filter(clusterTopic -> hasCollision(clusterTopic, topic.getMetadata().getName()))
                   .collect(Collectors.toList());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException(e.getMessage());
        }
    }

    /**
     * Validate existing topic can be updated with new given configs
     * @param existingTopic The existing topic
     * @param newTopic The new topic
     * @return A list of validation errors
     */
    public List<String> validateTopicUpdate(Namespace namespace, Topic existingTopic, Topic newTopic) {
        List<String> validationErrors = new ArrayList<>();

        if (existingTopic.getSpec().getPartitions() != newTopic.getSpec().getPartitions()) {
            validationErrors.add(String.format("Invalid value %s for configuration partitions: Value is immutable (%s).",
                    newTopic.getSpec().getPartitions(), existingTopic.getSpec().getPartitions()));
        }

        if (existingTopic.getSpec().getReplicationFactor() != newTopic.getSpec().getReplicationFactor()) {
            validationErrors.add(String.format("Invalid value %s for configuration replication.factor: Value is immutable (%s).",
                    newTopic.getSpec().getReplicationFactor(), existingTopic.getSpec().getReplicationFactor()));
        }

        Optional<KafkaAsyncExecutorConfig> topicCluster = kafkaAsyncExecutorConfig
                .stream()
                .filter(cluster -> namespace.getMetadata().getCluster().equals(cluster.getName()))
                .findFirst();

        boolean confluentCloudCluster = topicCluster.isPresent() && topicCluster.get().getProvider().equals(KafkaAsyncExecutorConfig.KafkaProvider.CONFLUENT_CLOUD);
        if (confluentCloudCluster && existingTopic.getSpec().getConfigs().get(CLEANUP_POLICY_CONFIG).equals(CLEANUP_POLICY_DELETE) &&
                newTopic.getSpec().getConfigs().get(CLEANUP_POLICY_CONFIG).equals(CLEANUP_POLICY_COMPACT)) {
            validationErrors.add(String.format("Invalid value %s for configuration cleanup.policy: Altering topic configuration from `delete` to `compact` is not currently supported. Please create a new topic with `compact` policy specified instead.",
                    newTopic.getSpec().getConfigs().get(CLEANUP_POLICY_CONFIG)));
        }

        return validationErrors;
    }

    /**
     * Check if topics collide with "_" instead of "."
     * @param topicA The first topic
     * @param topicB The second topic
     * @return true if it does, false otherwise
     */
    private boolean hasCollision(String topicA, String topicB) {
        return topicA.replace('.', '_').equals(topicB.replace('.', '_'));
    }

    /**
     * List the topics that are not synchronized to ns4kafka by namespace
     * @param namespace The namespace
     * @return The list of topics
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     * @throws TimeoutException Any timeout exception
     */
    public List<Topic> listUnsynchronizedTopics(Namespace namespace) throws ExecutionException, InterruptedException, TimeoutException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        // List topics for this namespace
        List<String> topicNames = listUnsynchronizedTopicNames(namespace);

        // Get topics definitions
        Collection<Topic> unsynchronizedTopics = topicAsyncExecutor.collectBrokerTopicsFromNames(topicNames)
                .values();

        return new ArrayList<>(unsynchronizedTopics);
    }

    /**
     * List the topic names that are not synchronized to ns4kafka by namespace
     * @param namespace The namespace
     * @return The list of topic names
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     * @throws TimeoutException Any timeout exception
     */
    public List<String> listUnsynchronizedTopicNames(Namespace namespace) throws ExecutionException, InterruptedException, TimeoutException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(namespace.getMetadata().getCluster()));

        return topicAsyncExecutor.listBrokerTopicNames()
                .stream()
                // ...that belongs to this namespace
                .filter(topic -> isNamespaceOwnerOfTopic(namespace.getMetadata().getName(), topic))
                // ...and aren't in ns4kafka storage
                .filter(topic -> findByName(namespace, topic).isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * Validate if a topic can be eligible for records deletion
     * @param deleteRecordsTopic The topic to delete records
     * @return A list of errors
     */
    public List<String> validateDeleteRecordsTopic(Topic deleteRecordsTopic) {
        List<String> errors = new ArrayList<>();

        if (deleteRecordsTopic.getSpec().getConfigs().get("cleanup.policy").equals("compact")) {
            errors.add("Cannot delete records on a compacted topic. Please delete and recreate the topic.");
        }

        return errors;
    }

    /**
     * For a given topic, get each latest offset by partition in order to delete all the records
     * before these offsets
     * @param topic The topic to delete records
     * @return A map of offsets by topic-partitions
     * @throws ExecutionException Any execution exception
     * @throws InterruptedException Any interrupted exception
     */
    public Map<TopicPartition, Long> prepareRecordsToDelete(Topic topic) throws ExecutionException, InterruptedException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(topic.getMetadata().getCluster()));

        try {
            return topicAsyncExecutor.prepareRecordsToDelete(topic.getMetadata().getName())
                    .entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, kv -> kv.getValue().beforeOffset()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException(e.getMessage());
        }
    }

    /**
     * Delete the records for each partition, before each offset
     * @param recordsToDelete The offsets by topic-partitions
     * @return The new offsets by topic-partitions
     * @throws InterruptedException Any interrupted exception
     */
    public Map<TopicPartition, Long> deleteRecords(Topic topic, Map<TopicPartition, Long> recordsToDelete) throws InterruptedException {
        TopicAsyncExecutor topicAsyncExecutor = applicationContext.getBean(TopicAsyncExecutor.class,
                Qualifiers.byName(topic.getMetadata().getCluster()));

        try {
            Map<TopicPartition, RecordsToDelete> recordsToDeleteMap = recordsToDelete.entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, kv -> RecordsToDelete.beforeOffset(kv.getValue())));

            return topicAsyncExecutor.deleteRecords(recordsToDeleteMap);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedException(e.getMessage());
        }
    }
}