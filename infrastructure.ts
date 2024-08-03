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
  aws_sns_subscriptions,
} from 'aws-cdk-lib'
import { Construct } from 'constructs'
import {
  webFileServeBucketName,
  WebStackDev,
  WebStack,
} from './infrastructure/web'
import { WebAppStack } from './infrastructure/web_app'

export class RaptorStatsStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props)

    const DATA_BUCKET = 'replays-processing'
    const bucketProps = {
      accessControl: aws_s3.BucketAccessControl.PRIVATE,
      blockPublicAccess: aws_s3.BlockPublicAccess.BLOCK_ALL,
      bucketName: DATA_BUCKET,
      encryption: aws_s3.BucketEncryption.S3_MANAGED,
      publicReadAccess: false,
      removalPolicy: RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
      versioned: false,
    }

    const dataBucket = new aws_s3.Bucket(this, DATA_BUCKET, {
      ...bucketProps,
      ...{ versioned: true },
    })
    const dataBucketDev = new aws_s3.Bucket(this, DATA_BUCKET + '-dev', {
      ...bucketProps,
      ...{
        bucketName: DATA_BUCKET + '-dev',
      },
    })
    dataBucket.addLifecycleRule({
      noncurrentVersionExpiration: Duration.days(21),
    })

    const fileServeBucket = aws_s3.Bucket.fromBucketName(
      this,
      'ImportedBucket',
      webFileServeBucketName,
    )

    const gcpSecret = aws_secretsmanager.Secret.fromSecretCompleteArn(
      this,
      'raptor-gcp',
      'arn:aws:secretsmanager:eu-north-1:190920611368:secret:raptor-gcp-x1EjkW',
    )

    const eventRuleRaptorStats = new aws_events.Rule(this, 'scheduleRule', {
      schedule: aws_events.Schedule.expression(
        // 'cron(0 */6 * * ? *)',
        'cron(0 1,4,7,10,13,16,19,22 * * ? *)',
      ),
    })

    assert(typeof process.env.DISCORD_USERNAME === 'string')
    assert(typeof process.env.ALARM_EMAIL === 'string')

    const imageAsset = (handler: string) =>
      aws_lambda.DockerImageCode.fromImageAsset(__dirname, {
        cmd: [`${handler}.main`],
        exclude: ['**/*.pyc', '**/__pycache__'],
      })

    const lambdaProps = {
      code: imageAsset('raptor_stats'),
      functionName: 'RaptorStats',
      environment: {
        DISCORD_USERNAME: process.env.DISCORD_USERNAME,
        DATA_BUCKET: dataBucket.bucketName,
      },
      timeout: Duration.seconds(900),
      memorySize: 2500,
      architecture: aws_lambda.Architecture.ARM_64,
      retryAttempts: 0,
      maxEventAge: Duration.minutes(5),
      logRetention: aws_logs.RetentionDays.ONE_MONTH,
    }
    const raptorStats = new aws_lambda.DockerImageFunction(
      this,
      'RaptorStats',
      lambdaProps,
    )
    dataBucket.grantReadWrite(raptorStats)
    dataBucketDev.grantReadWrite(raptorStats)
    fileServeBucket.grantWrite(raptorStats)
    eventRuleRaptorStats.addTarget(
      new aws_events_targets.LambdaFunction(raptorStats),
    )

    const pveRating = new aws_lambda.DockerImageFunction(this, 'PveRating', {
      ...lambdaProps,
      ...{
        code: imageAsset('pve_rating'),
        functionName: 'PveRating',
        timeout: Duration.seconds(900),
        memorySize: 1900,
      },
    })

    pveRating.grantInvoke(raptorStats)
    dataBucket.grantReadWrite(pveRating)
    dataBucketDev.grantReadWrite(pveRating)
    fileServeBucket.grantWrite(pveRating)

    const spreadsheet = new aws_lambda.DockerImageFunction(
      this,
      'Spreadsheet',
      {
        ...lambdaProps,
        ...{
          code: imageAsset('spreadsheet'),
          functionName: 'Spreadsheet',
          timeout: Duration.seconds(500),
          memorySize: 600,
        },
      },
    )

    spreadsheet.grantInvoke(raptorStats)
    spreadsheet.grantInvoke(pveRating)
    dataBucket.grantRead(spreadsheet)
    dataBucketDev.grantRead(spreadsheet)

    const recentGames = new aws_lambda.DockerImageFunction(
      this,
      'RecentGames',
      {
        ...lambdaProps,
        ...{
          code: imageAsset('recent_games'),
          functionName: 'RecentGames',
          timeout: Duration.seconds(500),
          memorySize: 2600,
        },
      },
    )

    recentGames.grantInvoke(pveRating)
    dataBucket.grantRead(recentGames)
    dataBucketDev.grantRead(recentGames)
    fileServeBucket.grantReadWrite(recentGames)

    const exceptionTopic = new aws_sns.Topic(this, 'lambda-exception-topic', {
      displayName: 'lambda-exception-topic',
      topicName: 'lambda-exception-topic',
    })

    exceptionTopic.addSubscription(
      new aws_sns_subscriptions.EmailSubscription(process.env.ALARM_EMAIL),
    )
    ;[raptorStats, pveRating, spreadsheet, recentGames].forEach((fun) => {
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
const stackProps = {
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
}
const app = new App()
new WebStack(app, 'WebStack', stackProps) // NOSONAR
new WebStackDev(app, 'WebStackDev', stackProps) // NOSONAR
new WebAppStack(app, 'WebAppStack', stackProps) // NOSONAR
new RaptorStatsStack(app, 'RaptorStatsStack', stackProps) // NOSONAR
// new ExperimentStack(app, 'ExperimentStack', stackProps) // NOSONAR
app.synth()
