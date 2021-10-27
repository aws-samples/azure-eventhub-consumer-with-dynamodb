import * as cdk from "@aws-cdk/core";
import * as ec2 from "@aws-cdk/aws-ec2";
import * as ecs from "@aws-cdk/aws-ecs";
import * as dynamodb from "@aws-cdk/aws-dynamodb";
import * as iam from "@aws-cdk/aws-iam";
import * as s3 from "@aws-cdk/aws-s3";

interface InfrastructureStackProps extends cdk.StackProps {
  readonly connectionString: string;
}

export class InfrastructureStack extends cdk.Stack {
  constructor(
    scope: cdk.Construct,
    id: string,
    props: InfrastructureStackProps
  ) {
    super(scope, id, props);

    // S3 bucket to dump events fetched from an Event Hub
    const rawDataBucket = new s3.Bucket(this, `RawDataBucket`, {
      encryption: s3.BucketEncryption.S3_MANAGED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,
    });

    // DDB table to store checkpoint and ownership data for consuming an Event Hub
    const checkpointTable = new dynamodb.Table(this, `CheckpointTable`, {
      partitionKey: {
        type: dynamodb.AttributeType.STRING,
        name: "pk",
      },
      sortKey: {
        type: dynamodb.AttributeType.STRING,
        name: "sk",
      },
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // VPC and ECS cluster to run an Event Hub consumer
    const vpc = new ec2.Vpc(this, "Vpc");
    const cluster = new ecs.Cluster(this, `Cluster`, { vpc });

    const taskDefinition = new ecs.FargateTaskDefinition(
      this,
      `TaskDefinition`,
      {
        memoryLimitMiB: 1024,
        cpu: 256,
      }
    );

    taskDefinition.addContainer(`container`, {
      image: ecs.ContainerImage.fromAsset("../backend", {
        ignoreMode: cdk.IgnoreMode.DOCKER,
      }),
      logging: new ecs.AwsLogDriver({
        streamPrefix: "/aws/ecs/eventhub-consumer",
      }),
      environment: {
        CONNECTION_STRING: props.connectionString,
        DESTINATION_BUCKET_NAME: rawDataBucket.bucketName,
        CHECKPOINT_TABLE_NAME: checkpointTable.tableName,
      },
    });

    const service = new ecs.FargateService(this, `Service`, {
      cluster,
      taskDefinition: taskDefinition,
      desiredCount: 1,
      minHealthyPercent: 100,
      circuitBreaker: { rollback: true },
    });

    rawDataBucket.grantWrite(service.taskDefinition.taskRole);
    checkpointTable.grantReadWriteData(service.taskDefinition.taskRole);
    taskDefinition.addToTaskRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["cloudwatch:PutMetricData"],
        resources: ["*"],
      })
    );

    {
      // CfnOutput section
      new cdk.CfnOutput(this, `EventHubConsumerOutputBucket`, {
        value: rawDataBucket.bucketArn,
      });
    }
  }
}
