import {
  CfnOutput,
  RemovalPolicy,
  Stack,
  StackProps,
  aws_certificatemanager,
  aws_s3,
} from 'aws-cdk-lib'
import {
  Distribution,
  OriginAccessIdentity,
  ViewerProtocolPolicy,
} from 'aws-cdk-lib/aws-cloudfront'
import { S3Origin } from 'aws-cdk-lib/aws-cloudfront-origins'
import { CanonicalUserPrincipal, PolicyStatement } from 'aws-cdk-lib/aws-iam'
import { BucketDeployment, Source } from 'aws-cdk-lib/aws-s3-deployment'
import { Construct } from 'constructs'

export class WebAppStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props)

    // Create an S3 bucket for the website
    const websiteBucket = new aws_s3.Bucket(this, 'stats-web', {
      blockPublicAccess: {
        blockPublicAcls: false,
        blockPublicPolicy: false,
        ignorePublicAcls: false,
        restrictPublicBuckets: false,
      },
      encryption: aws_s3.BucketEncryption.S3_MANAGED,
      removalPolicy: RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
      publicReadAccess: true,
      websiteIndexDocument: 'index.html',
    })

    const originAccessIdentity = new OriginAccessIdentity(
      this,
      'OriginAccessIdentity',
    )
    // websiteBucket.grantRead(originAccessIdentity)

    // Update the S3 bucket policy to allow CloudFront to read objects
    websiteBucket.addToResourcePolicy(
      new PolicyStatement({
        actions: ['s3:GetObject'],
        resources: [`${websiteBucket.bucketArn}/*`],
        principals: [
          new CanonicalUserPrincipal(
            originAccessIdentity.cloudFrontOriginAccessIdentityS3CanonicalUserId,
          ),
        ],
      }),
    )

    // Create a CloudFront distribution for the website
    const distribution = new Distribution(this, 'WebsiteDistribution', {
      comment: 'webapp prod',
      defaultBehavior: {
        origin: new S3Origin(websiteBucket, {
          originAccessIdentity,
        }),
        viewerProtocolPolicy: ViewerProtocolPolicy.REDIRECT_TO_HTTPS,
      },
      defaultRootObject: 'index.html',
      domainNames: ['pverating.bar', 'www.pverating.bar'],
      certificate: aws_certificatemanager.Certificate.fromCertificateArn(
        this,
        'Certificate',
        'arn:aws:acm:us-east-1:190920611368:certificate/44684981-fba0-47bc-8baa-9fe0aff29659',
      ),
    })

    // Deploy the built Vue.js files to the S3 bucket
    new BucketDeployment(this, 'DeployWebsite', {
      sources: [Source.asset('app/dist')],
      destinationBucket: websiteBucket,
      distribution,
      distributionPaths: ['/*'],
    })

    // Output the CloudFront distribution URL
    new CfnOutput(this, 'WebsiteURL', {
      value: distribution.distributionDomainName,
    })
  }
}
