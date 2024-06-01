import * as cdk from 'aws-cdk-lib'
import * as lambda from 'aws-cdk-lib/aws-lambda'
import { Construct } from 'constructs'

export class RaptorStatsStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props)

    const bucket = new cdk.aws_s3.Bucket(this, 'raptor-stats-parquet', {
      bucketName: 'raptor-stats-parquet',
      blockPublicAccess: cdk.aws_s3.BlockPublicAccess.BLOCK_ALL,
      accessControl: cdk.aws_s3.BucketAccessControl.PRIVATE,
      removalPolicy: cdk.RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
      encryption: cdk.aws_s3.BucketEncryption.S3_MANAGED,
      versioned: false,
    })
    const s3AccessPolicy = new cdk.aws_iam.PolicyStatement({
      actions: ['s3:*'],
      resources: [bucket.bucketArn + '/*'],
    })
    const gcpSecret = cdk.aws_secretsmanager.Secret.fromSecretCompleteArn(
      this,
      'raptor-gcp',
      'arn:aws:secretsmanager:eu-north-1:190920611368:secret:raptor-gcp-x1EjkW',
    )
    const eventRuleRaptorStats = new cdk.aws_events.Rule(this, 'scheduleRule', {
      schedule: cdk.aws_events.Schedule.expression('cron(*/20 * * * ? *)'),
    })

    const lambdaProps = {
      code: lambda.DockerImageCode.fromImageAsset(__dirname, {
        cmd: ['RaptorStats.lambda_handler.handler'],
      }),
      functionName: 'RaptorStats',
      environment: {
        BUCKET_NAME: bucket.bucketName,
      },
      timeout: cdk.Duration.seconds(500),
      memorySize: 1024,
      architecture: cdk.aws_lambda.Architecture.ARM_64,
      retryAttempts: 0,
      maxEventAge: cdk.Duration.minutes(5),
    }
    const raptorStats = new lambda.DockerImageFunction(
      this,
      'RaptorStats',
      lambdaProps,
    )
    bucket.grantReadWrite(raptorStats)
    raptorStats.addToRolePolicy(s3AccessPolicy)
    eventRuleRaptorStats.addTarget(
      new cdk.aws_events_targets.LambdaFunction(raptorStats),
    )
    gcpSecret.grantRead(raptorStats)

    const pveSkill = new lambda.DockerImageFunction(this, 'PveSkill', {
      ...lambdaProps,
      ...{
        code: lambda.DockerImageCode.fromImageAsset(__dirname, {
          cmd: ['PveSkill.lambda_handler.handler'],
        }),
        functionName: 'PveSkill',
        timeout: cdk.Duration.seconds(220),
        memorySize: 3008,
      },
    })

    const eventRulePveSkill = new cdk.aws_events.Rule(
      this,
      'scheduleRulePveSkill',
      {
        schedule: cdk.aws_events.Schedule.expression('cron(5,25,45 * * * ? *)'),
      },
    )

    eventRulePveSkill.addTarget(
      new cdk.aws_events_targets.LambdaFunction(pveSkill),
    )
    bucket.grantRead(pveSkill)
    pveSkill.addToRolePolicy(s3AccessPolicy)
    gcpSecret.grantRead(pveSkill)
  }
}

const app = new cdk.App()
new RaptorStatsStack(app, 'RaptorStatsStack', {
  /* If you don't specify 'env', this stack will be environment-agnostic.
   * Account/Region-dependent features and context lookups will not work,
   * but a single synthesized template can be deployed anywhere. */
  /* Uncomment the next line to specialize this stack for the AWS Account
   * and Region that are implied by the current CLI configuration. */
  // env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
  /* Uncomment the next line if you know exactly what Account and Region you
   * want to deploy the stack to. */
  env: { account: '190920611368', region: 'eu-north-1' },
  /* For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html */
})
app.synth()
