import { SynthUtils } from "@aws-cdk/assert";
import * as cdk from "@aws-cdk/core";
import { InfrastructureStack } from "../lib/infrastructure-stack";

test("Snapshot test", () => {
  const app = new cdk.App();
  const stack = new InfrastructureStack(app, "MyTestStack", {
    connectionString: "dummy",
  });
  expect(SynthUtils.toCloudFormation(stack)).toMatchSnapshot();
});
