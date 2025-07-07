import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { createPresignedPost, PresignedPost } from '@aws-sdk/s3-presigned-post';

import { DBOS, ArgOptional, ConfiguredInstance, WorkflowConfig } from '@dbos-inc/dbos-sdk';
import { AWSServiceConfig, getAWSConfigByName, getConfigForAWSService } from '@dbos-inc/aws-config';
import { Error as DBOSError } from '@dbos-inc/dbos-sdk';

export interface FileRecord {
  key: string;
}

export interface S3Callbacks {
  newActiveFile: (rec: FileRecord) => Promise<unknown>;
  newPendingFile: (rec: FileRecord) => Promise<unknown>;
  fileActivated: (rec: FileRecord) => Promise<unknown>;
  fileDeleted: (rec: FileRecord) => Promise<unknown>;
}

export interface DBOSS3Config {
  awscfgname?: string;
  awscfg?: AWSServiceConfig;
  bucket: string;
  s3Callbacks?: S3Callbacks;
}

interface S3GetResponseOptions {
  PartNumber?: number;
  Range?: string;

  RequestPayer?: 'requester' | undefined;

  IfMatch?: string;
  IfModifiedSince?: Date;
  IfNoneMatch?: string;
  IfUnmodifiedSince?: Date;

  ExpectedBucketOwner?: string;
  SSECustomerAlgorithm?: string;
  SSECustomerKey?: string;
  SSECustomerKeyMD5?: string;

  VersionId?: string;

  ResponseCacheControl?: string;
  ResponseContentDisposition?: string;
  ResponseContentEncoding?: string;
  ResponseContentLanguage?: string;
  ResponseContentType?: string;
  ResponseExpires?: Date;
}

export class DBOS_S3 extends ConfiguredInstance {
  //////////
  // S3 Configuration
  //////////

  static AWS_S3_CONFIGURATION = 'aws_s3_configuration';
  s3client?: S3Client = undefined;

  constructor(
    name: string,
    readonly config: DBOSS3Config,
  ) {
    super(name);
  }

  async initialize() {
    // Get the config and call the validation
    if (!this.config.awscfg) {
      if (this.config.awscfgname) {
        this.config.awscfg = getAWSConfigByName(this.config.awscfgname);
      } else {
        this.config.awscfg = getConfigForAWSService(DBOS_S3.AWS_S3_CONFIGURATION);
      }
    }
    this.s3client = DBOS_S3.createS3Client(this.config.awscfg);
    return Promise.resolve();
  }

  static createS3Client(cfg: AWSServiceConfig) {
    return new S3Client({
      endpoint: cfg.endpoint,
      region: cfg.region,
      credentials: cfg.credentials,
      maxAttempts: cfg.maxRetries,
    });
  }

  ///////
  //  Basic functions + step wrappers
  ///////

  // Delete object
  static async deleteS3Cmd(s3: S3Client, bucket: string, key: string) {
    return await s3.send(
      new DeleteObjectCommand({
        Bucket: bucket,
        Key: key,
      }),
    );
  }

  @DBOS.step()
  async delete(key: string) {
    return await DBOS_S3.deleteS3Cmd(this.s3client!, this.config.bucket, key);
  }

  // Put small string
  static async putS3Cmd(s3: S3Client, bucket: string, key: string, content: string, contentType: string) {
    return await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        ContentType: contentType,
        Body: content,
      }),
    );
  }

  @DBOS.step()
  async put(key: string, content: string, @ArgOptional contentType: string = 'text/plain') {
    return await DBOS_S3.putS3Cmd(this.s3client!, this.config.bucket, key, content, contentType);
  }

  // Get string
  static async getS3Cmd(s3: S3Client, bucket: string, key: string) {
    return await s3.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: key,
      }),
    );
  }

  @DBOS.step()
  async get(key: string) {
    return (await DBOS_S3.getS3Cmd(this.s3client!, this.config.bucket, key)).Body?.transformToString();
  }

  // Presigned GET key
  static async getS3KeyCmd(
    s3: S3Client,
    bucket: string,
    key: string,
    expirationSecs: number,
    options: S3GetResponseOptions = {},
  ) {
    const getObjectCommand = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
      ...options,
    });

    const presignedUrl = await getSignedUrl(s3, getObjectCommand, { expiresIn: expirationSecs });
    return presignedUrl;
  }

  @DBOS.step()
  async presignedGetURL(
    key: string,
    @ArgOptional expirationSecs: number = 3600,
    @ArgOptional options: S3GetResponseOptions = {},
  ) {
    return await DBOS_S3.getS3KeyCmd(this.s3client!, this.config.bucket, key, expirationSecs, options);
  }

  // Presigned post key
  static async postS3KeyCmd(
    s3: S3Client,
    bucket: string,
    key: string,
    expirationSecs: number,
    @ArgOptional
    contentOptions?: {
      contentType?: string;
      contentLengthMin?: number;
      contentLengthMax?: number;
    },
  ): Promise<PresignedPost> {
    const postPresigned = await createPresignedPost(s3, {
      Conditions: [
        ['content-length-range', contentOptions?.contentLengthMin ?? 1, contentOptions?.contentLengthMax ?? 10000000], // 10MB
      ],
      Bucket: bucket,
      Key: key,
      Expires: expirationSecs,
      Fields: {
        'Content-Type': contentOptions?.contentType ?? '*',
      },
    });
    return { url: postPresigned.url, fields: postPresigned.fields };
  }

  @DBOS.step()
  async createPresignedPost(
    key: string,
    expirationSecs: number = 1200,
    @ArgOptional
    contentOptions?: {
      contentType?: string;
      contentLengthMin?: number;
      contentLengthMax?: number;
    },
  ) {
    try {
      return await DBOS_S3.postS3KeyCmd(this.s3client!, this.config.bucket, key, expirationSecs, contentOptions);
    } catch (e) {
      DBOS.logger.error(e);
      throw new DBOSError.DBOSError(`Unable to presign post: ${(e as Error).message}`, 500);
    }
  }

  /////////
  // Simple Workflows
  /////////

  // Send a data string item directly to S3
  // Use cases for this:
  //  App code produces (or received from the user) _small_ data to store in S3
  //   One-shot workflow
  //     Do the S3 op
  //     If it succeeds, write in table
  //     If it fails drop the partial file (if any) from S3
  // Note this will ALL get logged in the DB as a workflow parameter (and a communicator parameter) so better not be big!
  @DBOS.workflow()
  async saveStringToFile(fileDetails: FileRecord, content: string, @ArgOptional contentType = 'text/plain') {
    // Running this as a communicator could possibly be skipped... but only for efficiency
    try {
      await this.put(fileDetails.key, content, contentType);
    } catch (e) {
      await this.delete(fileDetails.key);
      throw e;
    }

    await this.config.s3Callbacks?.newActiveFile(fileDetails);
    return fileDetails;
  }

  //  App code reads a file out of S3
  @DBOS.workflow()
  async readStringFromFile(fileDetails: FileRecord) {
    const txt = await this.get(fileDetails.key);
    return txt;
  }

  //  App code deletes a file out of S3
  //     Do the table write
  //     Do the S3 op
  @DBOS.workflow()
  async deleteFile(fileDetails: FileRecord) {
    await this.config.s3Callbacks?.fileDeleted(fileDetails);
    return await this.delete(fileDetails.key);
  }

  ////////////
  // Workflows where client goes direct to S3 (slightly more complicated)
  ////////////

  // There are cases where we don't want to store the data in the DB
  //   Especially end-user uploads, or cases where the file is accessed by outside systems

  //  Presigned D/L for end user
  @DBOS.workflow()
  async getFileReadURL(
    fileDetails: FileRecord,
    @ArgOptional expirationSec = 3600,
    @ArgOptional options: S3GetResponseOptions = {},
  ): Promise<string> {
    return await this.presignedGetURL(fileDetails.key, expirationSec, options);
  }
}

export interface S3WorkflowCallbacks<R extends FileRecord, Options = unknown> {
  // Database operations (these should be transactions)
  newActiveFile: (rec: R) => Promise<unknown>;
  newPendingFile: (rec: R) => Promise<unknown>;
  fileActivated: (rec: R) => Promise<unknown>;
  fileDeleted: (rec: R) => Promise<unknown>;

  // S3 interaction options, these will be run as steps
  createPresignedPost: (rec: R, timeout?: number, options?: Options) => Promise<PresignedPost>;
  validateS3Upload?: (rec: R) => Promise<void>;
  deleteS3Object: (rec: R) => Promise<void>;
}

export function registerS3PresignedUploadWorkflow<R extends FileRecord, Options = unknown>(
  options: {
    name?: string;
    ctorOrProto?: object;
    className?: string;
    config?: WorkflowConfig;
  },
  callbacks: S3WorkflowCallbacks<R, Options>,
) {
  return DBOS.registerWorkflow(async (fileDetails: R, timeoutSeconds: number, objOptions?: Options) => {
    await callbacks.newPendingFile(fileDetails);

    const upkey = await DBOS.runStep(
      async () => {
        return await callbacks.createPresignedPost(fileDetails, timeoutSeconds, objOptions);
      },
      { name: 'createPresignedPost' },
    );
    await DBOS.setEvent<PresignedPost>('uploadkey', upkey);

    try {
      const res = await DBOS.recv<boolean>('uploadfinish', timeoutSeconds + 60);

      if (!res) {
        throw new Error('S3 operation timed out or canceled');
      }

      // Validate the file, if we have code for that
      if (callbacks.validateS3Upload) {
        await DBOS.runStep(
          async () => {
            await callbacks.validateS3Upload!(fileDetails);
          },
          { name: 'validateFileUpload' },
        );
      }

      await callbacks?.fileActivated(fileDetails);
    } catch (e) {
      try {
        await callbacks.fileDeleted(fileDetails);
        await DBOS.runStep(
          async () => {
            await callbacks.deleteS3Object(fileDetails);
          },
          { name: 'deleteS3Object' },
        );
      } catch (e2) {
        DBOS.logger.debug(e2);
      }
      throw e;
    }

    return fileDetails;
  }, options);
}
