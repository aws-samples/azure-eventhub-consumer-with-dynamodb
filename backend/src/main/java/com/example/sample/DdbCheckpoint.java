package com.example.sample;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import com.azure.messaging.eventhubs.models.Checkpoint;

@DynamoDbBean
public class DdbCheckpoint {
    private String pk;
    private String sk;

    private String fullyQualifiedNamespace;
    private String eventHubName;
    private String consumerGroup;
    private String partitionId;
    private Long offset;
    private Long sequenceNumber;

    public static DdbCheckpoint copyFrom(Checkpoint original) {
        DdbCheckpoint cp = new DdbCheckpoint();
        cp.setConsumerGroup(original.getConsumerGroup());
        cp.setEventHubName(original.getEventHubName());
        cp.setFullyQualifiedNamespace(original.getFullyQualifiedNamespace());
        cp.setOffset(original.getOffset());
        cp.setPartitionId(original.getPartitionId());
        cp.setSequenceNumber(original.getSequenceNumber());
        cp.setPk(buildPk(cp.getFullyQualifiedNamespace(), cp.getEventHubName(), cp.getConsumerGroup()));
        cp.setSk(cp.getPartitionId());

        return cp;
    }

    public static String buildPk(String fullyQualifiedNamespace, String eventHubName, String consumerGroup) {
        return String.format("cp#%s#%s#%s", fullyQualifiedNamespace, eventHubName, consumerGroup);
    }

    public Checkpoint export() {
        Checkpoint cp = new Checkpoint();
        cp.setConsumerGroup(this.getConsumerGroup());
        cp.setEventHubName(this.getEventHubName());
        cp.setFullyQualifiedNamespace(this.getFullyQualifiedNamespace());
        cp.setOffset(this.getOffset());
        cp.setPartitionId(this.getPartitionId());
        cp.setSequenceNumber(this.getSequenceNumber());

        return cp;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(Long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(String partitionId) {
        this.partitionId = partitionId;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getEventHubName() {
        return eventHubName;
    }

    public void setEventHubName(String eventHubName) {
        this.eventHubName = eventHubName;
    }

    public String getFullyQualifiedNamespace() {
        return fullyQualifiedNamespace;
    }

    public void setFullyQualifiedNamespace(String fullyQualifiedNamespace) {
        this.fullyQualifiedNamespace = fullyQualifiedNamespace;
    }

    @DynamoDbPartitionKey
    public String getPk() {
        return this.pk;
    };

    @DynamoDbSortKey
    public String getSk() {
        return this.sk;
    }

    public void setPk(String pk) {
        this.pk = pk;
    }

    public void setSk(String sk) {
        this.sk = sk;
    }

}
