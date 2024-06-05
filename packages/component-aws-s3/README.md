# DBOS AWS Simple Storage Service (S3) Component

This is a [DBOS](https://docs.dbos.dev/) library for working with [Amazon Web Services Simple Storage Service (S3)](https://aws.amazon.com/s3/).

## Getting Started
In order to store and retrieve objects in S3, it is necessary to:
- Register with AWS, create an S3 bucket, and create access credentials. (See [Getting started with Amazon S3](https://aws.amazon.com/s3/getting-started/) in AWS documentation.)
- If browser-based clients will read from S3 directly, set up S3 CORS.  (See [Using cross-origin resource sharing (CORS)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html) in AWS documentation.)

## Configuring a DBOS Application with AWS S3
First, ensure that the DBOS S3 component is installed into the application:
```
npm install --save @dbos-inc/component-aws-s3
```

Second, ensure that the library class is imported and exported from an application entrypoint source file:
```typescript
import { S3Ops } from "@dbos-inc/component-aws-s3";
export { S3Ops };
```

Third, place appropriate configuration into the [`dbos-config.yaml`](https://docs.dbos.dev/api-reference/configuration) file; the following example will pull the AWS information from the environment:
```yaml
application:
  aws_s3_configuration: aws_config # Optional if the section is called `aws_config`
  aws_config:
    aws_region: ${AWS_REGION}
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

If a different configuration file section should be used for SES, the `aws_s3_configuration` can be changed to indicate a configuration section for use with SES.  If multiple configurations are to be used, the application code will have to name and configure them.

The application will likely need an s3 bucket.  This can be placed in the `application` section of `dbos-config.yaml` also, but the naming key is to be established by the application.

## Selecting A Configuration
`S3Ops` is a configured class.  This means that the configuration (or config file key name) must be provided when a class instance is created, for example:
```typescript
const defaultS3 = configureInstance(S3Ops, 'default', {awscfgname: 'aws_config', bucket: 'my-s3-bucket', ...});
```

## Simple Operation Wrappers

## Consistently Maintaining a Database Table of S3 Objects

## Reading and Writing S3 From DBOS Workflows

## Client Access To S3 Objects

## Simple Testing
The `s3_utils.test.ts` file included in the source repository can be used to send an email and a templated email.  Before running, set the following environment variables:
- `S3_BUCKET`: The S3 bucket for setting / retrieving test objects
- `AWS_REGION`: AWS region to use
- `AWS_ACCESS_KEY_ID`: The access key with permission to use the SES service
- `AWS_SECRET_ACCESS_KEY`: The secret access key corresponding to `AWS_ACCESS_KEY_ID`

## Next Steps
- For a detailed DBOS Transact tutorial, check out our [programming quickstart](https://docs.dbos.dev/getting-started/quickstart-programming).
- To learn how to deploy your application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/getting-started/quickstart-cloud/)
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
