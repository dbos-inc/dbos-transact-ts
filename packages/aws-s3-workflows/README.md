# DBOS AWS Simple Storage Service (S3) Workflows

This is a [DBOS](https://docs.dbos.dev/) workflow library for working with [Amazon Web Services Simple Storage Service (S3)](https://aws.amazon.com/s3/). The primary feature of this library is workflows that keep a database table in sync with the S3 contents, regardless of failures. These workflows can be used as-is, or as an example for your own code.

## Keeping S3 And Database In Sync

AWS applications often store large / unstructured data as S3 objects. S3 is, as the name implies, simple storage, and it doesn't comprehensively track file attributes, permissions, fine-grained ownership, dependencies, etc., and listing the objects can be inefficient and expensive.

Keeping an indexed set of file metadata records, including referential links to their owners, is a "database problem". And, while keeping the database in sync with the contents of S3 sounds like it may be tricky, [DBOS Workflows](https://docs.dbos.dev/typescript/tutorials/workflow-tutorial) provide the perfect tool for accomplishing this, even in the face of client or server failures.

When keeping a database table in sync with S3 contents, several failures have to be anticipated:

1. The S3 object could get added without a database record getting added, perhaps due to a machine failure during processing. This would create an orphaned S3 object.
2. Likewise, the a database table entry could get removed, without the S3 object getting cleaned up.
3. An outside process responsible for putting an object into S3 could fail, leaving the S3 object state in an unknown condition.

This library solves all three of these problems via lightweight, durable DBOS workflows. DBOS workflows ensure that execution resumes, finishing the job of adding or removing the database table entries, or performing cleanup if an outside process does not indicate success within a timeout period.

## Getting Started

To use AWS S3:

- Register with AWS, create an S3 bucket, and create access credentials. (See [Getting started with Amazon S3](https://aws.amazon.com/s3/getting-started/) in AWS documentation.)
- If browser-based clients will read from S3 directly, set up S3 CORS. (See [Using cross-origin resource sharing (CORS)](https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html) in AWS documentation.)

## Using AWS S3 From a DBOS Application

First, ensure that the AWS S3 SDK and DBOS S3 workflows are installed into the application:

```
npm install --save @dbos-inc/aws-s3-workflows aws-sdk/s3-presigned-post @aws-sdk/s3-request-presigner
```

Second, ensure that workflow registration functions are imported:

```typescript
import {
  FileRecord,
  S3WorkflowCallbacks,
  registerS3UploadWorkflow,
  registerS3PresignedUploadWorkflow,
  registerS3DeleteWorkflow,
} from '@dbos-inc/aws-s3-workflows';
```

Third, create your database transactions, and which will be used in the callback for the S3 workflows. The transactions below are just examples, you can use your own table schema, data source, and migrations.

```typescript
// See the test for schema, migrations, file lookup SQL, etc...
class TestUserFileTable {
  // File table DML operations
  @DBOS.transaction()
  static async insertFileRecord(rec: UserFile) {
    await DBOS.knexClient<FileDetails>('user_files').insert(TestUserFileTable.toFileDetails(rec));
  }
  @DBOS.transaction()
  static async updateFileRecord(rec: UserFile) {
    await DBOS.knexClient<FileDetails>('user_files')
      .update(TestUserFileTable.toFileDetails(rec))
      .where({ file_id: rec.file_id });
  }
  @DBOS.transaction()
  static async deleteFileRecordById(file_id: string) {
    await DBOS.knexClient<FileDetails>('user_files').delete().where({ file_id });
  }
  // Lookups, etc. ...
}
```

Fourth, create an callback for the workflows to use. The callback will specify the transactions and exact S3 calls to make, as these can be customized based on the method used for obtaining an S3 client.

```typescript
const s3callback: S3WorkflowCallbacks<UserFile, Opts> = {
  // Database operations (these should be transactions)
  newActiveFile: async (rec: UserFile) => {
    rec.file_status = FileStatus.ACTIVE;
    return await TestUserFileTable.insertFileRecord(rec);
  },
  newPendingFile: async (rec: UserFile) => {
    rec.file_status = FileStatus.PENDING;
    return await TestUserFileTable.insertFileRecord(rec);
  },
  fileActivated: async (rec: UserFile) => {
    rec.file_status = FileStatus.ACTIVE;
    return await TestUserFileTable.updateFileRecord(rec);
  },
  fileDeleted: async (rec: UserFile) => {
    return await TestUserFileTable.deleteFileRecordById(rec.file_id);
  },

  // S3 interaction options, these will be run as steps
  putS3Contents: async (rec: UserFile, content: string, options?: Opts) => {
    return await s3client?.send(
      new PutObjectCommand({
        Bucket: s3bucket,
        Key: rec.key,
        ContentType: options?.contentType ?? 'text/plain',
        Body: content,
      }),
    );
  },
  createPresignedPost: async (rec: UserFile, timeout?: number, opts?: Opts) => {
    const postPresigned = await createPresignedPost(s3client!, {
      Conditions: [
        ['content-length-range', 1, 10000000], // 10MB
      ],
      Bucket: s3bucket!,
      Key: rec.key,
      Expires: timeout || 60,
      Fields: {
        'Content-Type': opts?.contentType || '*',
      },
    });
    return { url: postPresigned.url, fields: postPresigned.fields };
  },
  deleteS3Object: async (rec: UserFile) => {
    return await s3client?.send(
      new DeleteObjectCommand({
        Bucket: s3bucket,
        Key: rec.key,
      }),
    );
  },
};
```

Finally, register and use the S3 workflows:

```typescript
export const uploadWF = registerS3UploadWorkflow({ className: 'UserFile', name: 'uploadWF' }, s3callback);
...
await uploadWF(myFileRec, 'This is my file');
```

### Workflow to Upload a String to S3

The `registerS3UploadWorkflow` function registers a workflow that stores a string to an S3 key, and runs the callback function to update the database. If anything goes wrong during the workflow, S3 will be cleaned up and the database will be unchanged by the workflow.

```typescript
export const uploadWF = registerS3UploadWorkflow({ className: 'UserFile', name: 'uploadWF' }, s3callback);
...
await uploadWF(myFileRec, 'These contents should be saved to S3');
```

This workflow performs the following actions:

- Puts the string in S3
- If there is difficulty with S3, ensures that no entry is left there and throws an error
- Invokes the transaction for a new active file record

### Workflow to Delete a File

The `registerS3DeleteWorkflow` function creates a workflow that removes a file from both S3 and the database.

```typescript
export const deleteWF = registerS3DeleteWorkflow({ className: 'UserFile', name: 'deleteWF' }, s3callback);

await deleteWF(fileDBRecord);
```

This workflow performs the following actions:

- Invokes the callback transaction for a deleting the file record
- Removes the key from S3

### Workflow to Allow Client File Upload via Presigned URLs

It is often not convenient to send or retrieve large S3 object contents through DBOS; the client should exchange data directly with S3. S3 accomodates this use case very well, using a feature called ["presigned URLs"](https://docs.aws.amazon.com/AmazonS3/latest/userguide/PresignedUrlUploadObject.html).

In these cases, the client can place a request to DBOS that produces a presigned POST URL, which the client can use for a limited time and purpose for S3 access. The DBOS workflow takes care of making sure that table entries are cleaned up if the client does not finish writing files.

The workflow interaction generally proceeds as follows:

- The client makes some request to a DBOS API handler.
- The handler decides that the client will be uploading a file, and starts the upload workflow.
- The upload workflow sends the handler a presigned post, which the handler returns to the client. The workflow continues in the background, waiting to hear that the client upload is complete.
- The client (ideally) uploads the file using the presigned post and notifies the application; if not, the workflow times out and cleans up. (The workflow timeout should be set to occur after the presigned post expires.)
- If successful, the database is updated to reflect the new file, otherwise S3 is cleaned of any partial work.

The workflow can be initiated in the following way:

```typescript
export const uploadPWF = registerS3PresignedUploadWorkflow({ className: 'UserFile', name: 'uploadPWF' }, s3callback);

// Start the workflow
const wfHandle = await DBOS.startWorkflow(uploadPWF)(
  myFileRec,
  60, // Expiration (seconds)
  { contentType: 'text/plain' },
);

// Get the presigned post (to pass to client)
const ppost = await DBOS.getEvent<PresignedPost>(wfHandle.workflowID, 'uploadkey');
```

The client will then use the presigned post to upload data to S3 directly:

```typescript
// Upload to the URL from the presigned post
const res = await uploadToS3(ppost!, '/path/to/file');
```

Upon a completion call from the client, the workflow should be notified so that it proceeds:

```typescript
// Look up wfHandle by the workflow ID
const wfHandle = DBOS.retrieveWorkflow(wfid);

// Notify workflow - truish means success, any falsy value indicates failure / cancel
await DBOS.send<boolean>(wfHandle.workflowID(), true, 'uploadfinish');

// Optionally, await completion of the workflow; this ensures that the database record is written,
//  or will throw an error if anything went wrong in the workflow
await wfHandle.getResult();
```

## Notes

_Do not reuse S3 keys._ Assigning unique identifiers to files is a much better idea, if a "name" is to be reused, it can be reused in the lookup database.

Reasons why S3 keys should not be reused:

- S3 caches the key contents. Even a response of "this key doesn't exist" can be cached. If you reuse keys, you may get a stale value.
- Workflow operations against an old use of a key may still be in process... for example a delete workflow may still be attempting to delete the old object at the same time a new file is being placed under the same key.

## Simple Testing

The `s3_utils.test.ts` file included in the source repository can be used to upload and download files to/from S3 using various approaches. Before running, set the following environment variables:

- `S3_BUCKET`: The S3 bucket for setting / retrieving test objects
- `AWS_REGION`: AWS region to use
- `AWS_ACCESS_KEY_ID`: The access key with permission to use the S3 service
- `AWS_SECRET_ACCESS_KEY`: The secret access key corresponding to `AWS_ACCESS_KEY_ID`

The test illustrates S3 setup, use of the workflows, and some S3 helper functions.

## Next Steps

- To start a DBOS app from a template, visit our [quickstart](https://docs.dbos.dev/quickstart).
- For DBOS programming tutorials, check out our [programming guide](https://docs.dbos.dev/typescript/programming-guide).
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact-ts).
