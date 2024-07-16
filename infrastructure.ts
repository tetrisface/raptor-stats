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

    const DATA_BUCKET = 'replays-processing'
    const bucketProps = {
      bucketName: DATA_BUCKET,
      blockPublicAccess: aws_s3.BlockPublicAccess.BLOCK_ALL,
      accessControl: aws_s3.BucketAccessControl.PRIVATE,
      removalPolicy: RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
      encryption: aws_s3.BucketEncryption.S3_MANAGED,
      versioned: false,
    }

    const dataBucket = new aws_s3.Bucket(this, DATA_BUCKET, bucketProps)
    const dataBucketDev = new aws_s3.Bucket(this, DATA_BUCKET + '-dev', {
      ...bucketProps,
      ...{
        bucketName: DATA_BUCKET + '-dev',
      },
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
        // 'cron(0 */2 * * ? *)',
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
        DISCORD_USERNAME: process.env.DISCORD_USERNAME,
        DATA_BUCKET: `s3://${dataBucket.bucketName}/`,
      },
      timeout: Duration.seconds(900),
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
    dataBucket.grantReadWrite(raptorStats)
    dataBucketDev.grantReadWrite(raptorStats)
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
        memorySize: 1500,
      },
    })

    pveRating.grantInvoke(raptorStats)
    dataBucket.grantReadWrite(pveRating)
    dataBucketDev.grantReadWrite(pveRating)
    webBucket.grantWrite(pveRating)

    const spreadsheet = new lambda.DockerImageFunction(this, 'Spreadsheet', {
      ...lambdaProps,
      ...{
        code: lambda.DockerImageCode.fromImageAsset(__dirname, {
          cmd: ['Spreadsheet.lambda_handler.handler'],
        }),
        functionName: 'Spreadsheet',
        timeout: Duration.seconds(500),
        memorySize: 600,
      },
    })

    spreadsheet.grantInvoke(raptorStats)
    spreadsheet.grantInvoke(pveRating)
    dataBucket.grantRead(spreadsheet)
    dataBucketDev.grantRead(spreadsheet)

    const exceptionTopic = new aws_sns.Topic(this, 'lambda-exception-topic', {
      displayName: 'lambda-exception-topic',
      topicName: 'lambda-exception-topic',
    })

    ;[raptorStats, pveRating, spreadsheet].forEach((fun) => {
      fun
        .metricErrors({
          period: Duration.minutes(1),
        })
        .createAlarm(
          this,
          `${fun.functionName.toLowerCase()}-exception-alarm`,
          {
            threshold: 1,
            evaluationPeriods: 1,
            alarmDescription: `Exception on ${fun.functionName}`,
            treatMissingData: aws_cloudwatch.TreatMissingData.IGNORE,
          },
        )
        .addAlarmAction(new aws_cloudwatch_actions.SnsAction(exceptionTopic))
      if (fun.functionName !== 'Spreadsheet') {
        fun.logGroup.addMetricFilter(`-log-filter`, {
          filterPattern: {
            logPatternString: 'not casted cols',
          },
          metricNamespace: fun.functionName,
          metricName: `${fun.functionName}-col-alarm`,
          metricValue: '1',
        })
        fun
          .metricErrors({
            period: Duration.minutes(1),
          })
          .createAlarm(this, `${fun.functionName.toLowerCase()}-col-alarm`, {
            threshold: 1,
            evaluationPeriods: 1,
            alarmDescription: `Exception on ${fun.functionName}`,
            treatMissingData: aws_cloudwatch.TreatMissingData.IGNORE,
          })
          .addAlarmAction(new aws_cloudwatch_actions.SnsAction(exceptionTopic))
        gcpSecret.grantRead(fun)
      }
    })
  }
}
const env = { account: '190920611368', region: 'eu-north-1' }
const app = new App()
new WebStack(app, 'WebStack', { env }) // NOSONAR
const stackProps = {
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
}
new RaptorStatsStack(app, 'RaptorStatsStack', stackProps) // NOSONAR
app.synth()
