import {
  RemovalPolicy,
  Stack,
  StackProps,
  aws_cloudfront,
  aws_cloudfront_origins,
  aws_s3,
} from 'aws-cdk-lib'
import { Construct } from 'constructs'

const bucketName = 'pve-rating-web'

export class WebStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props)
    const bucket = new aws_s3.Bucket(this, 'pve-rating-web', {
      accessControl: aws_s3.BucketAccessControl.PRIVATE,

      bucketName: 'pve-rating-web',
      blockPublicAccess: aws_s3.BlockPublicAccess.BLOCK_ALL,
      removalPolicy: RemovalPolicy.RETAIN_ON_UPDATE_OR_DELETE,
      encryption: aws_s3.BucketEncryption.S3_MANAGED,
      versioned: false,
    })

    const originAccessIdentity = new aws_cloudfront.OriginAccessIdentity(
      this,
      'OriginAccessIdentity',
    )
    bucket.grantRead(originAccessIdentity)

    new aws_cloudfront.Distribution(this, 'Distribution', {
      defaultRootObject: 'pve_ratings.json',
      defaultBehavior: {
        origin: new aws_cloudfront_origins.S3Origin(bucket, {
          originAccessIdentity,
        }),
        responseHeadersPolicy: new aws_cloudfront.ResponseHeadersPolicy(
          this,
          'ResponseHeadersPolicy',
          {
            customHeadersBehavior: {
              customHeaders: [
                {
                  header: 'Content-Type',
                  value: 'application/json',
                  override: true,
                },
              ],
            },
          },
        ),
      },
      // domainNames: ['pverating.bar', 'www.pverating.bar'],
      // certificate: new aws_certificatemanager.Certificate(
      //   this,
      //   'CustomDomainCertificate',
      //   {
      //     domainName: '*.pverating.bar',
      //     validation: aws_certificatemanager.CertificateValidation.fromDns(),
      //     subjectAlternativeNames: ['pverating.bar', 'www.pverating.bar'],
      //   },
      // ),
    })
  }
}
export default bucketName
