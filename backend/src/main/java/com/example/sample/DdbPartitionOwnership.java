package com.example.sample;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

import com.azure.messaging.eventhubs.models.PartitionOwnership;

@DynamoDbBean
public class DdbPartitionOwnership {
    private String pk;
    private String sk;

    private String fullyQualifiedNamespace;
    private String eventHubName;
    private String consumerGroup;
    private String partitionId;
    private String ownerId;
    private Long lastModifiedTime;
    private String eTag;

    public static DdbPartitionOwnership copyFrom(PartitionOwnership original) {
        DdbPartitionOwnership po = new DdbPartitionOwnership();
        po.setConsumerGroup(original.getConsumerGroup());
        po.setEventHubName(original.getEventHubName());
        po.setFullyQualifiedNamespace(original.getFullyQualifiedNamespace());
        po.setETag(original.getETag());
        po.setPartitionId(original.getPartitionId());
        po.setLastModifiedTime(original.getLastModifiedTime());
        po.setOwnerId(original.getOwnerId());
        po.setPk(buildPk(po.getFullyQualifiedNamespace(), po.getEventHubName(), po.getConsumerGroup()));
        po.setSk(po.getPartitionId());

        return po;
    }

    public static String buildPk(String fullyQualifiedNamespace, String eventHubName, String consumerGroup) {
        return String.format("po#%s#%s#%s", fullyQualifiedNamespace, eventHubName, consumerGroup);
    }

    public PartitionOwnership export() {
        PartitionOwnership po = new PartitionOwnership();
        po.setConsumerGroup(this.getConsumerGroup());
        po.setEventHubName(this.getEventHubName());
        po.setFullyQualifiedNamespace(this.getFullyQualifiedNamespace());
        po.setETag(this.getETag());
        po.setPartitionId(this.getPartitionId());
        po.setLastModifiedTime(this.getLastModifiedTime());
        po.setOwnerId(this.getOwnerId());

        return po;
    }

    public String getETag() {
        return eTag;
    }

    public void setETag(String eTag) {
        this.eTag = eTag;
    }

    public Long getLastModifiedTime() {
        return lastModifiedTime;
    }

    public void setLastModifiedTime(Long lastModifiedTime) {
        this.lastModifiedTime = lastModifiedTime;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
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