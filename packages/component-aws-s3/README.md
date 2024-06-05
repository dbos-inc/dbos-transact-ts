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
The `S3Ops` class provides several [communicator](https://docs.dbos.dev/tutorials/communicator-tutorial) wrappers for S3 functions.

### Reading and Writing S3 From DBOS Handlers and Workflows

#### Writing S3 Objects
A string can be written to an S3 key with the following:
```typescript
    const putres = await ctx.invoke(defaultS3).putS3Comm('/my/test/key', "Test string from DBOS");
```

### Reading S3 Objects
A string can be read from an S3 key with the following:
```typescript
    const getres = await ctx.invoke(defaultS3).getS3Comm('/my/test/key');
```

### Deleting Objects
An S3 key can be removed/deleted with the following:
```typescript
    const delres = await ctx.invoke(defaultS3).deleteS3Comm('/my/test/key');
```

### Client Access To S3 Objects
As shown below, DBOS provides a convenient and powerful way to track and manage S3 objects.  However, it is often not convenient to send or retrieve large S3 object contents through DBOS; it would be preferable to have the client talk directly to S3.  S3 accomodates this use case very well, using a feature called ["presigned URLs"](https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html).

In these cases, the client can place a request to DBOS that produces a presigned GET / POST URL, which the client can use for a limited time and purpose for S3 access.

#### Presigned GET URLs
A presigned GET URL can be created for an S3 key with the following:
```typescript
const geturl = await ctx.invoke(defaultS3).getS3KeyComm('/my/test/key', 30 /*expiration, in seconds*/);
```

The resulting URL string can be used in the same way as any other URL for placing HTTP GET requests.

#### Presigned POSTs
A presigned POST URL can be created for an S3 key with the following:
```typescript
const presignedPost = await ctx.invoke(defaultS3).postS3KeyComm(
    '/my/test/key', 30/*expiration*/, {contentType: 'text/plain'}/*size/content restrictions*/);
```

The resulting `PresignedPost` object is slightly more involved than a regular URL, as it contains not just URL to be used, but also the required HTTP headers that must accompany the POST request.  An example of using the `PresignedPost` in Typescript with [Axios](https://axios-http.com/docs/intro) is:
```typescript
    async function uploadToS3(presignedPostData: PresignedPost, filePath: string) {
        const formData = new FormData();

        // Append all the fields from the presigned post data
        Object.keys(presignedPostData.fields).forEach(key => {
            formData.append(key, presignedPostData.fields[key]);
        });

        // Append the file you want to upload
        const fileStream = fs.createReadStream(filePath);
        formData.append('file', fileStream);

        // Access the presigned post URL
        return await axios.post(presignedPostData.url, formData);
    }
```

## Consistently Maintaining a Database Table of S3 Objects

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
