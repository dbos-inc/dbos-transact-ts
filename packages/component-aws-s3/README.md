# DBOS AWS Simple Storage Service (S3) Component

This is a [DBOS](https://docs.dbos.dev/) library for working with [Amazon Web Services Simple Storage Service (S3)](https://aws.amazon.com/s3/).

## Getting Started

In order to store and retrieve objects in S3, it is necessary to:

- Register with AWS, create an S3 bucket, and create access credentials. (See [Getting started with Amazon S3](https://aws.amazon.com/s3/getting-started/) in AWS documentation.)
- If browser-based clients will read from S3 directly, set up S3 CORS. (See [Using cross-origin resource sharing (CORS)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html) in AWS documentation.)

## Configuring a DBOS Application with AWS S3

First, ensure that the DBOS S3 component is installed into the application:

```
npm install --save @dbos-inc/component-aws-s3
```

Second, ensure that the library class is imported and exported from an application entrypoint source file:

```typescript
import { DBOS_S3 } from '@dbos-inc/component-aws-s3';
export { DBOS_S3 };
```

Third, place appropriate configuration into the [`dbos-config.yaml`](https://docs.dbos.dev/typescript/reference/configuration) file; the following example will pull the AWS information from the environment:

```yaml
application:
  aws_s3_configuration: aws_config # Optional if the section is called `aws_config`
  aws_config:
    aws_region: ${AWS_REGION}
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

If a different configuration file section should be used for S3, the `aws_s3_configuration` can be changed to indicate a configuration section for use with S3. If multiple configurations are to be used, the application code will have to name and configure them.

The application will need at least one s3 bucket. This can be placed in the `application` section of `dbos-config.yaml` also, but the naming key is to be established by the application.

## Selecting A Configuration

`DBOS_S3` is a configured class. The AWS configuration (or config file key name) and bucket name must be provided when a class instance is created, for example:

```typescript
const defaultS3 = new DBOS_S3('myS3Bucket', {awscfgname: 'aws_config', bucket: 'my-s3-bucket', ...});
```

## Simple Operation Wrappers

The `DBOS_S3` class provides several DBOS [step](https://docs.dbos.dev/typescript/tutorials/step-tutorial) wrappers for S3 functions.

### Reading and Writing S3 From DBOS Handlers and Workflows

#### Writing S3 Objects

A string can be written to an S3 key with the following:

```typescript
const putres = await defaultS3.put('/my/test/key', 'Test string from DBOS');
```

Note that the arguments to `put` will be logged to the database. Consider [having the client upload to S3 with a presigned post](#client-access-to-s3-objects) if the data is generated outside of DBOS or if the data is larger than a few megabytes.

### Reading S3 Objects

A string can be read from an S3 key with the following:

```typescript
const getres = await defaultS3.get('/my/test/key');
```

Note that the return value from `get` will be logged to the database. Consider [reading directly from S3 with a signed URL](#presigned-get-urls) if the data is large.

### Deleting Objects

An S3 key can be removed/deleted with the following:

```typescript
const delres = await defaultS3.delete('/my/test/key');
```

### Client Access To S3 Objects

As shown below, DBOS provides a convenient and powerful way to track and manage S3 objects. However, it is often not convenient to send or retrieve large S3 object contents through DBOS; it would be preferable to have the client talk directly to S3. S3 accomodates this use case very well, using a feature called ["presigned URLs"](https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html).

In these cases, the client can place a request to DBOS that produces a presigned GET / POST URL, which the client can use for a limited time and purpose for S3 access.

#### Presigned GET URLs

A presigned GET URL can be created for an S3 key with the following:

```typescript
const geturl = await defaultS3.presignedGetURL('/my/test/key', 30 /*expiration, in seconds*/);
```

The resulting URL string can be used in the same way as any other URL for placing HTTP GET requests.

It may be desired to return the `Content-Type`, `Content-Disposition`, or other headers to the client that ultimately makes the S3 GET request. This can be controlled by providing [response options](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-client-s3/Interface/GetObjectCommandInput/).

```typescript
// Let the client know it is downloading a .zip file
const geturl = await defaultS3.presignedGetURL('/my/test/key', 30 /*expiration, in seconds*/, {
  ResponseContentType: 'application/zip',
  ResponseContentDisposition: `attachment; filename="file.zip"`,
});
```

#### Presigned POSTs

A presigned POST URL can be created for an S3 key with the following:

```typescript
const presignedPost = await defaultS3.createPresignedPost(
  '/my/test/key',
  30 /*expiration*/,
  { contentType: 'text/plain' } /*size/content restrictions*/,
);
```

The resulting `PresignedPost` object is slightly more involved than a regular URL, as it contains not just URL to be used, but also the required HTTP headers that must accompany the POST request. An example of using the `PresignedPost` in Typescript with [Axios](https://axios-http.com/docs/intro) is:

```typescript
async function uploadToS3(presignedPostData: PresignedPost, filePath: string) {
  const formData = new FormData();

  // Append all the fields from the presigned post data
  Object.keys(presignedPostData.fields).forEach((key) => {
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

In many cases, an application wants to keep track of objects that have been stored in S3. S3 is, as the name implies, simple storage, and it doesn't track file attributes, permissions, fine-grained ownership, dependencies, etc.

Keeping an indexed set of file metadata records, including referential links to their owners, is a "database problem". And, while keeping the database in sync with the contents of S3 sounds like it may be tricky, [DBOS Transact Workflows](https://docs.dbos.dev/typescript/tutorials/workflow-tutorial) provide the perfect tool for accomplishing this, even in the face of client or server failures.

The `DBOS_S3` class provides workflows that can be used to ensure that a table of file records is maintained for an S3 bucket. This table can have any schema suitable to the application (an example table schema can be found in `s3_utils.test.ts`), because the application provides the code to maintain it as a set of callback functions that will be triggered from the workflow.

The interface for the workflow functions (described below) allows for the following callbacks:

```typescript
export interface FileRecord {
  key: string; // AWS S3 Key
}

export interface DBOSS3Config {
  s3Callbacks?: {
    /* Called when a new active file is added to S3 */
    newActiveFile: (rec: FileRecord) => Promise<unknown>;

    /* Called when a new pending (still undergoing workflow processing) file is added to S3 */
    newPendingFile: (rec: FileRecord) => Promise<unknown>;

    /* Called when pending file becomes active */
    fileActivated: (rec: FileRecord) => Promise<unknown>;

    /* Called when a file is deleted from S3 */
    fileDeleted: (rec: FileRecord) => Promise<unknown>;
  };
  //... remainder of S3 Config
}
```

### Workflow to Upload a String to S3

The `saveStringToFile` workflow stores a string to an S3 key, and runs the callback function to update the database. If anything goes wrong during the workflow, S3 will be cleaned up and the database will be unchanged by the workflow.

```typescript
await defaultS3.saveStringToFile(fileDBRecord, 'This is my file');
```

This workflow performs the following actions:

- Puts the string in S3
- If there is difficulty with S3, ensures that no entry is left there and throws an error
- Invokes the callback for a new active file record

Note that the arguments to `saveStringToFile` will be logged to the database. Consider [having the client upload to S3 with a presigned post](#workflow-to-allow-client-file-upload) if the data is larger than a few megabytes.

### Workflow to Retrieve a String from S3

The workflow function `readStringFromFile(fileDetails: FileRecord)` will retrieve the contents of an S3 object as a `string`.

This workflow currently performs no additional operations outside of a call to `get(fileDetails.key)`.

Note that the return value from `readStringFromFile` will be logged to the database. Consider [having the client read from S3 with a signed URL](#workflow-to-allow-client-file-download) if the data is larger than a few megabytes.

### Workflow to Delete a File

The `deleteFile` workflow function removes a file from both S3 and the database.

```typescript
await defaultS3.deleteFile(fileDBRecord);
```

This workflow performs the following actions:

- Invokes the callback for a deleted file record
- Removes the key from S3

### Workflow to Allow Client File Upload

The workflow to allow application clients to upload to S3 directly is more involved than other workflows, as it runs concurrently with the client. The workflow interaction generally proceeds as follows:

- The client makes some request to a DBOS handler.
- The handler decides that the client will be uploading a file, and starts the upload workflow.
- The upload workflow sends the handler a presigned post, which the handler returns to the client. The workflow continues in the background, waiting to hear that the client upload is complete.
- The client (ideally) uploads the file using the presigned post and notifies the application; if not, the workflow times out and cleans up. (The workflow timeout should be set to occur after the presigned post expires.)
- If successful, the database is updated to reflect the new file, otherwise S3 is cleaned of any partial work.

The workflow can be used in the following way:

```typescript
// Start the workflow
const wfHandle = await DBOS.startWorkflow(defaultS3).writeFileViaURL(
  fileDBRec,
  60 /*expiration*/,
  { contentType: 'text/plain' } /*content restrictions*/,
);

// Get the presigned post from the workflow
const ppost = await DBOS.getEvent<PresignedPost>(wfHandle.workflowID, 'uploadkey');

// Return the ppost object to the client for use as in 'Presigned POSTs' section above
// The workflow ID should also be known in some way
```

Upon a completion call from the client, the following should be performed to notify the workflow to proceed:

```typescript
// Look up wfHandle by the workflow ID
const wfHandle = DBOS.retrieveWorkflow(wfid);

// Notify workflow - truish means success, any falsy value indicates failure / cancel
await DBOS.send<boolean>(wfHandle.workflowID(), true, 'uploadfinish');

// Optionally, await completion of the workflow; this ensures that the database record is written,
//  or will throw an error if anything went wrong in the workflow
await wfHandle.getResult();
```

### Workflow to Allow Client File Download

The workflow function `getFileReadURL(fileDetails: FileRecord, expiration: number, options: S3GetResponseOptions)` returns a signed URL for retrieving object contents from S3, valid for `expiration` seconds, and using any provided [response options](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-client-s3/Interface/GetObjectCommandInput/).

This workflow currently performs no additional operations outside of a call to `presignedGetURL(fileDetails.key, expiration, options)`.

## Notes

Do not reuse S3 keys. Assigning unique identifiers to files is a much better idea, if a "name" is to be reused, it can be reused in the lookup database. Reasons why S3 keys should not be reused:

- S3 caches the key contents. Even a response of "this key doesn't exist" can be cached. If you reuse keys, you may get a stale value.
- Workflow operations against an old use of a key may still be in process... for example a delete workflow may still be attempting to delete the old object at the same time a new file is being placed under the same key.

DBOS Transact logs function parameters and return values to the system database. Some functions above treat the data contents of the S3 object as a parameter or return value, and are therefore only suitable for small data items (kilobytes, maybe megabytes, not gigabytes). For large data, use workflows where the data is sent directly to and from S3 using presigned URLs.

## Simple Testing

The `s3_utils.test.ts` file included in the source repository can be used to upload and download files to/from S3 using various approaches. Before running, set the following environment variables:

- `S3_BUCKET`: The S3 bucket for setting / retrieving test objects
- `AWS_REGION`: AWS region to use
- `AWS_ACCESS_KEY_ID`: The access key with permission to use the S3 service
- `AWS_SECRET_ACCESS_KEY`: The secret access key corresponding to `AWS_ACCESS_KEY_ID`

## Next Steps

- To start a DBOS app from a template, visit our [quickstart](https://docs.dbos.dev/quickstart).
- For DBOS Transact programming tutorials, check out our [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
