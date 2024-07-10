import * as assert from 'assert'
import {
  App,
  Duration,
  RemovalPolicy,
  Stack,
  StackProps,
  aws_cloudwatch,
  aws_cloudwatch_actions,
  aws_events,
  aws_events_targets,
  aws_lambda,
  aws_logs,
  aws_s3,
  aws_secretsmanager,
  aws_sns,
} from 'aws-cdk-lib'
import * as lambda from 'aws-cdk-lib/aws-lambda'
import { Construct } from 'constructs'
import bucketName, { WebStack } from './infrastructure/web'

export class RaptorStatsStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props)

    const parquetBucket = new aws_s3.Bucket(this, 'raptor-stats-parquet', {
      bucketName: 'raptor-stats-parquet',
      blockPublicAccess: aws_s3.BlockPublicAccess.BLOCK_ALL,
      accessControl: aws_s3.BucketAccessControl.PRIVATE,
      removalPolicy: RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
      encryption: aws_s3.BucketEncryption.S3_MANAGED,
      versioned: false,
    })

    const webBucket = aws_s3.Bucket.fromBucketName(
      this,
      'ImportedBucket',
      bucketName,
    )

    const gcpSecret = aws_secretsmanager.Secret.fromSecretCompleteArn(
      this,
      'raptor-gcp',
      'arn:aws:secretsmanager:eu-north-1:190920611368:secret:raptor-gcp-x1EjkW',
    )

    const eventRuleRaptorStats = new aws_events.Rule(this, 'scheduleRule', {
      schedule: aws_events.Schedule.expression(
        'cron(0 1,4,7,10,13,16,19,22 * * ? *)',
      ),
    })

    assert(typeof process.env.DISCORD_USERNAME === 'string')

    const lambdaProps = {
      code: lambda.DockerImageCode.fromImageAsset(__dirname, {
        cmd: ['RaptorStats.lambda_handler.handler'],
      }),
      functionName: 'RaptorStats',
      environment: {
        BUCKET_NAME: parquetBucket.bucketName,
        DISCORD_USERNAME: process.env.DISCORD_USERNAME,
      },
      timeout: Duration.seconds(500),
      memorySize: 1700,
      architecture: aws_lambda.Architecture.ARM_64,
      retryAttempts: 0,
      maxEventAge: Duration.minutes(5),
      logRetention: aws_logs.RetentionDays.ONE_MONTH,
    }
    const raptorStats = new lambda.DockerImageFunction(
      this,
      'RaptorStats',
      lambdaProps,
    )
    parquetBucket.grantReadWrite(raptorStats)
    webBucket.grantWrite(raptorStats)
    eventRuleRaptorStats.addTarget(
      new aws_events_targets.LambdaFunction(raptorStats),
    )

    const pveRating = new lambda.DockerImageFunction(this, 'PveRating', {
      ...lambdaProps,
      ...{
        code: lambda.DockerImageCode.fromImageAsset(__dirname, {
          cmd: ['PveRating.lambda_handler.handler'],
        }),
        functionName: 'PveRating',
        timeout: Duration.seconds(900),
        memorySize: 3000,
      },
    })

    pveRating.grantInvoke(raptorStats)
    parquetBucket.grantReadWrite(pveRating)
    webBucket.grantWrite(pveRating)

    const spreadsheet = new lambda.DockerImageFunction(this, 'Spreadsheet', {
      ...lambdaProps,
      ...{
        code: lambda.DockerImageCode.fromImageAsset(__dirname, {
          cmd: ['Spreadsheet.lambda_handler.handler'],
        }),
        functionName: 'Spreadsheet',
        timeout: Duration.seconds(900),
        memorySize: 700,
      },
    })

    spreadsheet.grantInvoke(pveRating)
    spreadsheet.grantInvoke(raptorStats)
    parquetBucket.grantRead(spreadsheet)

    const exceptionTopic = new aws_sns.Topic(this, 'lambda-exception-topic', {
      displayName: 'lambda-exception-topic',
      topicName: 'lambda-exception-topic',
    })

    ;[
      { fun: raptorStats, name: 'raptorStats' },
      { fun: pveRating, name: 'pveRating' },
      { fun: spreadsheet, name: 'Spreadsheet' },
    ].forEach(({ fun, name }) => {
      fun
        .metricErrors({
          period: Duration.minutes(1),
        })
        .createAlarm(this, `${name.toLowerCase()}-exception-alarm`, {
          threshold: 1,
          evaluationPeriods: 1,
          alarmDescription: `Exception on ${name}`,
          treatMissingData: aws_cloudwatch.TreatMissingData.IGNORE,
        })
        .addAlarmAction(new aws_cloudwatch_actions.SnsAction(exceptionTopic))
      gcpSecret.grantRead(fun)
    })
  }
}
const env = { account: '190920611368', region: 'eu-north-1' }
const app = new App()
new WebStack(app, 'WebStack', { env })
new RaptorStatsStack(app, 'RaptorStatsStack', {
  /* If you don't specify 'env', this stack will be environment-agnostic.
   * Account/Region-dependent features and context lookups will not work,
   * but a single synthesized template can be deployed anywhere. */
  /* Uncomment the next line to specialize this stack for the AWS Account
   * and Region that are implied by the current CLI configuration. */
  // env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
  /* Uncomment the next line if you know exactly what Account and Region you
   * want to deploy the stack to. */
  env,
  /* For more information, see https://docs.aws.amazon.com/cdk/latest/guide/environments.html */
})
app.synth()
