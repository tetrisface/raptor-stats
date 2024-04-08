import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as path from "path";
import { Construct } from "constructs";

export class CdkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The code that defines your stack goes here

    // The python3.12 enabled Lambda Function
    const lambdaFunction = new lambda.Function(this, "raptor-stats-lambda", {
      runtime: lambda.Runtime.PYTHON_3_12,
      memorySize: 128,
      code: lambda.Code.fromAsset(path.join(__dirname, "/../lambda")),
      handler: "lambda_handler.handler",
    });
  }
}
