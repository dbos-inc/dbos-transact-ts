import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { createPresignedPost, PresignedPost } from '@aws-sdk/s3-presigned-post';

import {
  DBOS,
  ArgOptional,
  Communicator,
  CommunicatorContext,
  ConfiguredInstance,
  InitContext,
  Workflow,
  WorkflowContext,
} from '@dbos-inc/dbos-sdk';
import {
  AWSServiceConfig,
  getAWSConfigByName,
  getAWSConfigForService,
  getConfigForAWSService,
  loadAWSConfigByName,
} from '@dbos-inc/aws-config';
import { Error as DBOSError } from '@dbos-inc/dbos-sdk';

export interface FileRecord {
  key: string;
}

export interface DBOSS3Config {
  awscfgname?: string;
  awscfg?: AWSServiceConfig;
  bucket: string;
  s3Callbacks?: {
    newActiveFile: (rec: FileRecord) => Promise<unknown>;
    newPendingFile: (rec: FileRecord) => Promise<unknown>;
    fileActivated: (rec: FileRecord) => Promise<unknown>;
    fileDeleted: (rec: FileRecord) => Promise<unknown>;
  };
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
  //  Basic functions +
  //  Communicator wrappers for basic functions
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

  //  Presigned U/L for end user
  //    A workflow that creates a presigned post
  //    Sets that back for the caller
  //    Waits for a completion notification
  //    If it gets it, adds the DB entry
  //      The poll will end significantly after the S3 post URL expires
  //    This supports an OAOO key on the workflow.
  //      (Won't start that completion checker more than once)
  //      A repeat request will get the same presigned post URL
  @DBOS.workflow()
  async writeFileViaURL(
    fileDetails: FileRecord,
    @ArgOptional expirationSec = 3600,
    @ArgOptional
    contentOptions?: {
      contentType?: string;
      contentLengthMin?: number;
      contentLengthMax?: number;
    },
  ) {
    await this.config.s3Callbacks?.newPendingFile(fileDetails);

    const upkey = await this.createPresignedPost(fileDetails.key, expirationSec, contentOptions);
    await DBOS.setEvent<PresignedPost>('uploadkey', upkey);

    try {
      const res = await DBOS.recv<boolean>('uploadfinish', expirationSec + 60); // 1 minute extra?

      if (!res) {
        throw new Error('S3 operation timed out or canceled');
      }
      // TODO: Validate the file
      await this.config.s3Callbacks?.fileActivated(fileDetails);
    } catch (e) {
      try {
        const _cwfh = await DBOS.startWorkflow(this).deleteFile(fileDetails);
        // No reason to await result
      } catch (e2) {
        DBOS.logger.debug(e2);
      }
      throw e;
    }

    return fileDetails;
  }
}

export interface S3Config {
  awscfgname?: string;
  awscfg?: AWSServiceConfig;
  bucket: string;
  s3Callbacks?: {
    newActiveFile: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
    newPendingFile: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
    fileActivated: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
    fileDeleted: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
  };
}

export class S3Ops extends ConfiguredInstance {
  //////////
  // S3 Configuration
  //////////

  s3client?: S3Client = undefined;

  constructor(
    name: string,
    readonly config: S3Config,
  ) {
    super(name);
  }

  async initialize(ctx: InitContext) {
    // Get the config and call the validation
    if (!this.config.awscfg) {
      if (this.config.awscfgname) {
        this.config.awscfg = loadAWSConfigByName(ctx, this.config.awscfgname);
      } else {
        this.config.awscfg = getAWSConfigForService(ctx, DBOS_S3.AWS_S3_CONFIGURATION);
      }
    }
    this.s3client = S3Ops.createS3Client(this.config.awscfg);
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
  //  Basic functions +
  //  Communicator wrappers for basic functions
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

  @Communicator()
  async delete(_ctx: CommunicatorContext, key: string) {
    return await S3Ops.deleteS3Cmd(this.s3client!, this.config.bucket, key);
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

  @Communicator()
  async put(_ctx: CommunicatorContext, key: string, content: string, @ArgOptional contentType: string = 'text/plain') {
    return await S3Ops.putS3Cmd(this.s3client!, this.config.bucket, key, content, contentType);
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

  @Communicator()
  async get(_ctx: CommunicatorContext, key: string) {
    return (await S3Ops.getS3Cmd(this.s3client!, this.config.bucket, key)).Body?.transformToString();
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

  @Communicator()
  async presignedGetURL(
    _ctx: CommunicatorContext,
    key: string,
    @ArgOptional expirationSecs: number = 3600,
    @ArgOptional options: S3GetResponseOptions = {},
  ) {
    return await S3Ops.getS3KeyCmd(this.s3client!, this.config.bucket, key, expirationSecs, options);
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

  @Communicator()
  async createPresignedPost(
    ctx: CommunicatorContext,
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
      return await S3Ops.postS3KeyCmd(this.s3client!, this.config.bucket, key, expirationSecs, contentOptions);
    } catch (e) {
      ctx.logger.error(e);
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
  @Workflow()
  async saveStringToFile(
    ctx: WorkflowContext,
    fileDetails: FileRecord,
    content: string,
    @ArgOptional contentType = 'text/plain',
  ) {
    // Running this as a communicator could possibly be skipped... but only for efficiency
    try {
      await ctx.invoke(this).put(fileDetails.key, content, contentType);
    } catch (e) {
      await ctx.invoke(this).delete(fileDetails.key);
      throw e;
    }

    await this.config.s3Callbacks?.newActiveFile(ctx, fileDetails);
    return fileDetails;
  }

  //  App code reads a file out of S3
  @Workflow()
  async readStringFromFile(ctx: WorkflowContext, fileDetails: FileRecord) {
    const txt = await ctx.invoke(this).get(fileDetails.key);
    return txt;
  }

  //  App code deletes a file out of S3
  //     Do the table write
  //     Do the S3 op
  @Workflow()
  async deleteFile(ctx: WorkflowContext, fileDetails: FileRecord) {
    await this.config.s3Callbacks?.fileDeleted(ctx, fileDetails);
    return await ctx.invoke(this).delete(fileDetails.key);
  }

  ////////////
  // Workflows where client goes direct to S3 (slightly more complicated)
  ////////////

  // There are cases where we don't want to store the data in the DB
  //   Especially end-user uploads, or cases where the file is accessed by outside systems

  //  Presigned D/L for end user
  @Workflow()
  async getFileReadURL(
    ctx: WorkflowContext,
    fileDetails: FileRecord,
    @ArgOptional expirationSec = 3600,
    @ArgOptional options: S3GetResponseOptions = {},
  ): Promise<string> {
    return await ctx.invoke(this).presignedGetURL(fileDetails.key, expirationSec, options);
  }

  //  Presigned U/L for end user
  //    A workflow that creates a presigned post
  //    Sets that back for the caller
  //    Waits for a completion notification
  //    If it gets it, adds the DB entry
  //      The poll will end significantly after the S3 post URL expires
  //    This supports an OAOO key on the workflow.
  //      (Won't start that completion checker more than once)
  //      A repeat request will get the same presigned post URL
  @Workflow()
  async writeFileViaURL(
    ctx: WorkflowContext,
    fileDetails: FileRecord,
    @ArgOptional expirationSec = 3600,
    @ArgOptional
    contentOptions?: {
      contentType?: string;
      contentLengthMin?: number;
      contentLengthMax?: number;
    },
  ) {
    await this.config.s3Callbacks?.newPendingFile(ctx, fileDetails);

    const upkey = await ctx.invoke(this).createPresignedPost(fileDetails.key, expirationSec, contentOptions);
    await ctx.setEvent<PresignedPost>('uploadkey', upkey);

    try {
      const res = await ctx.recv<boolean>('uploadfinish', expirationSec + 60); // 1 minute extra?

      if (!res) {
        throw new Error('S3 operation timed out or canceled');
      }
      // TODO: Validate the file
      await this.config.s3Callbacks?.fileActivated(ctx, fileDetails);
    } catch (e) {
      try {
        const _cwfh = await ctx.startWorkflow(this).deleteFile(fileDetails);
        // No reason to await result
      } catch (e2) {
        ctx.logger.debug(e2);
      }
      throw e;
    }

    return fileDetails;
  }
}
