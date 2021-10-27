#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "@aws-cdk/core";
import { InfrastructureStack } from "../lib/infrastructure-stack";

const eventHubConnectionString = process.env.CONNECTION_STRING;

if (eventHubConnectionString == null) {
  throw Error("You must set CONNECTION_STRING environment variable.");
}

const app = new cdk.App();
new InfrastructureStack(app, "EventHubConsumerStack", {
  connectionString: eventHubConnectionString,
});
