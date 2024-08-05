import {
  Duration,
  RemovalPolicy,
  Stack,
  StackProps,
  aws_certificatemanager,
  aws_cloudfront,
  aws_cloudfront_origins,
  aws_s3,
} from 'aws-cdk-lib'
import { Construct } from 'constructs'

export const webFileServeBucketName = 'pve-rating-web-file-serve'
export class WebStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props)
    const bucket = new aws_s3.Bucket(
      this,
      webFileServeBucketName + (id === 'WebStack' ? '' : '-dev'),
      {
        accessControl: aws_s3.BucketAccessControl.PRIVATE,
        blockPublicAccess: aws_s3.BlockPublicAccess.BLOCK_ALL,
        bucketName: webFileServeBucketName + (id === 'WebStack' ? '' : '-dev'),
        encryption: aws_s3.BucketEncryption.S3_MANAGED,
        removalPolicy: RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
        versioned: false,
      },
    )
    bucket.addCorsRule({
      allowedHeaders: ['*'],
      allowedMethods: [aws_s3.HttpMethods.GET],
      allowedOrigins: ['*'],
      exposedHeaders: [],
      maxAge: 3000,
    })

    const cachePolicy = new aws_cloudfront.CachePolicy(
      this,
      'FileServeCachePolicy',
      {
        defaultTtl: Duration.hours(3),
        maxTtl: Duration.hours(3),
      },
    )

    const cachePolicyDev = new aws_cloudfront.CachePolicy(
      this,
      'FileServeCachePolicyDev',
      {
        defaultTtl: Duration.minutes(2),
        maxTtl: Duration.minutes(2),
        minTtl: Duration.seconds(10),
      },
    )

    const originAccessIdentity = new aws_cloudfront.OriginAccessIdentity(
      this,
      'OriginAccessIdentity',
    )
    bucket.grantRead(originAccessIdentity)

    new aws_cloudfront.Distribution(this, 'Distribution', {
      comment: 'file serve ' + (id === 'WebStack' ? 'prod' : 'dev'),
      defaultBehavior: {
        origin: new aws_cloudfront_origins.S3Origin(bucket, {
          originAccessIdentity,
        }),
        cachePolicy: id === 'WebStack' ? cachePolicy : cachePolicyDev,
        viewerProtocolPolicy:
          aws_cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
      },
      domainNames:
        id === 'WebStack'
          ? ['files.pverating.bar']
          : ['dev.files.pverating.bar'],
      certificate: aws_certificatemanager.Certificate.fromCertificateArn(
        this,
        'Certificate',
        'arn:aws:acm:us-east-1:190920611368:certificate/44684981-fba0-47bc-8baa-9fe0aff29659',
      ),
    })
  }
}

export class WebStackDev extends WebStack {}
