package com.example.sample;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import com.azure.core.util.CoreUtils;
import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.models.Checkpoint;
import com.azure.messaging.eventhubs.models.PartitionOwnership;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

public class DdbCheckpointStore implements CheckpointStore {
    private final Logger logger = Logger.getLogger("DdbCheckpointStore");
    private final DynamoDbTable<DdbCheckpoint> checkpointTable;
    private final DynamoDbTable<DdbPartitionOwnership> ownershipTable;

    public DdbCheckpointStore(final String tableName) {
        final DynamoDbClient ddb = DynamoDbClient.builder().build();
        final DynamoDbEnhancedClient client = DynamoDbEnhancedClient.builder().dynamoDbClient(ddb).build();

        // Create a DynamoDbTable object
        checkpointTable = client.table(tableName, TableSchema.fromBean(DdbCheckpoint.class));
        ownershipTable = client.table(tableName, TableSchema.fromBean(DdbPartitionOwnership.class));
    }

    @Override
    public Flux<PartitionOwnership> listOwnership(final String fullyQualifiedNamespace, final String eventHubName,
            final String consumerGroup) {
        logger.info("Listing partition ownership");

        final String prefix = DdbPartitionOwnership.buildPk(fullyQualifiedNamespace, eventHubName, consumerGroup);
        final PageIterable<DdbPartitionOwnership> pages = ownershipTable.query(
                r -> r.queryConditional(QueryConditional.keyEqualTo(Key.builder().partitionValue(prefix).build())));

        return Flux.fromIterable(pages.items()).map(r -> r.export());
    }

    @Override
    public Flux<PartitionOwnership> claimOwnership(final List<PartitionOwnership> requestedPartitionOwnerships) {
        if (CoreUtils.isNullOrEmpty(requestedPartitionOwnerships)) {
            return Flux.empty();
        }
        return Flux.fromIterable(requestedPartitionOwnerships).flatMap(partitionOwnership -> {
            logger.info(String.format("Ownership of partition %s claimed by %s", partitionOwnership.getPartitionId(),
                    partitionOwnership.getOwnerId()));

            String oldEtag = partitionOwnership.getETag();
            if (oldEtag == null) {
                oldEtag = "";
            }
            partitionOwnership.setETag(UUID.randomUUID().toString()).setLastModifiedTime(System.currentTimeMillis());

            final DdbPartitionOwnership po = DdbPartitionOwnership.copyFrom(partitionOwnership);
            logger.info("putitem start " + oldEtag + "  " + po.getOwnerId());

            final Expression ex1 = Expression.builder().expression("attribute_not_exists(ETag) OR ETag = :eTag")
                    .expressionValues(Map.of(":eTag", AttributeValue.builder().s(oldEtag).build())).build();

            try {
                ownershipTable.putItem(PutItemEnhancedRequest.builder(DdbPartitionOwnership.class).item(po)
                        .conditionExpression(ex1).build());
            } catch (final ConditionalCheckFailedException e) {
                // Etag doesn't match, possibly conflicting request for the partition ownership
                return Mono.empty();
            } catch (final Exception e) {
                logger.severe(e.toString());
                return Mono.empty();
            }
            return Mono.just(partitionOwnership);
        });
    }

    @Override
    public Flux<Checkpoint> listCheckpoints(final String fullyQualifiedNamespace, final String eventHubName, final String consumerGroup) {
        final String prefix = DdbCheckpoint.buildPk(fullyQualifiedNamespace, eventHubName, consumerGroup);
        final PageIterable<DdbCheckpoint> pages = checkpointTable.query(
                r -> r.queryConditional(QueryConditional.keyEqualTo(Key.builder().partitionValue(prefix).build())));

        return Flux.fromIterable(pages.items()).map(r -> r.export());
    }

    @Override
    public Mono<Void> updateCheckpoint(final Checkpoint checkpoint) {
        logger.info("updating checkpoint");
        if (checkpoint == null) {
            return Mono.error(new NullPointerException("checkpoint cannot be null"));
        }

        final DdbCheckpoint cp = DdbCheckpoint.copyFrom(checkpoint);
        checkpointTable.putItem(cp);

        logger.info(String.format("Updated checkpoint for partition %s with sequence number %s",
                checkpoint.getPartitionId(), checkpoint.getSequenceNumber()));
        return Mono.empty();
    }
}
