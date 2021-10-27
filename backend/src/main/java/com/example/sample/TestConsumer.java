package com.example.sample;

import java.time.Duration;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class TestConsumer {

    private final static String CONNECTION_STRING = System.getenv("CONNECTION_STRING");
    private final static String DESTINATION_BUCKET_NAME = System.getenv("DESTINATION_BUCKET_NAME");
    private final static String CHECKPOINT_TABLE_NAME = System.getenv("CHECKPOINT_TABLE_NAME");
    private final static int BATCH_SIZE = 10;
    private final static Duration BATCH_TIMEOUT = Duration.ofMillis(1000);

    private EventProcessorClient eventProcessorClient;
    public static void main(final String[] args) {
        new TestConsumer().start();
    }

    private void start() {
        final Logger logger = LoggerFactory.getLogger(TestConsumer.class);

        final S3Client s3 = S3Client.builder()
                .build();

        final CloudWatchReporter reporter = new CloudWatchReporter();
        reporter.start();
        final EventProcessorClientBuilder eventProcessorClientBuilder = new EventProcessorClientBuilder()
                .consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
                .connectionString(CONNECTION_STRING)
                .processEventBatch((eventBatchContext)-> {
                    final long startTime =  System.currentTimeMillis();
                    if (eventBatchContext.getEvents() == null || eventBatchContext.getEvents().isEmpty()) {
                        return;
                    }

                    final PutObjectRequest objectRequest = PutObjectRequest.builder()
                            .bucket(DESTINATION_BUCKET_NAME)
                            .key(UUID.randomUUID().toString())
                            .build();
                    final StringBuilder sb = new StringBuilder(); // we assume data is string here.
                    for (final EventData ed : eventBatchContext.getEvents()) {
                        sb.append(ed.getBodyAsString());
                        sb.append("\n");
                        reporter.report(startTime - ed.getEnqueuedTime().toEpochMilli());
                    }

                    s3.putObject(objectRequest, RequestBody.fromString(sb.toString()));
                    eventBatchContext.updateCheckpoint();
                }, BATCH_SIZE, BATCH_TIMEOUT)
                .processError(errorContext -> {
                    logger.error("## error, {} {}", errorContext.getPartitionContext().getPartitionId(), errorContext.getThrowable().getMessage());
                    logger.warn("Kill the task..");
                    System.exit(1);

                })
                .checkpointStore(new DdbCheckpointStore(CHECKPOINT_TABLE_NAME));

        eventProcessorClient = eventProcessorClientBuilder.buildEventProcessorClient();
        logger.info("## Starting event processor");
        eventProcessorClient.start();
    }

}
