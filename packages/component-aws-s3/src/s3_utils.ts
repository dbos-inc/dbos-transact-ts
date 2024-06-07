import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { createPresignedPost, PresignedPost } from '@aws-sdk/s3-presigned-post';

import {
    ArgOptional,
    Communicator, CommunicatorContext,
    ConfiguredInstance,
    InitContext,
    Workflow,
    WorkflowContext
} from '@dbos-inc/dbos-sdk';
import { AWSServiceConfig, getAWSConfigForService, loadAWSConfigByName } from '@dbos-inc/aws-config';
import { DBOSError } from '@dbos-inc/dbos-sdk/dist/src/error';

export interface FileRecord {
    key: string;
}

export interface S3Config{
    awscfgname?: string,
    awscfg?: AWSServiceConfig,
    bucket: string,
    s3Callbacks?: {
        newActiveFile: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
        newPendingFile: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
        fileActivated: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
        fileDeleted: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
    }
}

export class S3Ops extends ConfiguredInstance {
    //////////
    // S3 Configuration
    //////////

    static AWS_S3_CONFIGURATION = 'aws_s3_configuration';

    constructor(name: string, readonly config: S3Config) {super(name);}

    async initialize(ctx: InitContext) {
        // Get the config and call the validation
        if (!this.config.awscfg) {
            if (this.config.awscfgname) {
                this.config.awscfg = loadAWSConfigByName(ctx, this.config.awscfgname);
            }
            else {
                this.config.awscfg = getAWSConfigForService(ctx, S3Ops.AWS_S3_CONFIGURATION);
            }
        }
        return Promise.resolve();
    }

    static createS3Client(cfg: AWSServiceConfig) {
        return new S3Client({
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
    static async deleteS3Cmd(s3: S3Client, bucket: string, key: string)
    {
        return await s3.send(new DeleteObjectCommand({
            Bucket: bucket,
            Key: key,
        }));
    }

    static async deleteS3(awscfg: AWSServiceConfig, bucket: string, key: string)
    {
        const s3 = S3Ops.createS3Client(awscfg);

        return await S3Ops.deleteS3Cmd(s3, bucket, key);
    }

    @Communicator()
    async deleteS3Comm(_ctx: CommunicatorContext, key: string)
    {
        const cfg = this.config;
        return await S3Ops.deleteS3(cfg.awscfg!, cfg.bucket, key);
    }

    // Put small string
    static async putS3Cmd(s3: S3Client, bucket: string, key: string, content: string, contentType: string)
    {
        return await s3.send(new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            ContentType: contentType,
            Body: content,
        }));
    }

    static async putS3(awscfg: AWSServiceConfig, bucket: string, key: string, content: string, contentType: string)
    {
        const s3 = S3Ops.createS3Client(awscfg);

        return await S3Ops.putS3Cmd(s3, bucket, key, content, contentType);
    }

    @Communicator()
    async putS3Comm(_ctx: CommunicatorContext, key: string, content: string, @ArgOptional contentType: string = 'text/plain')
    {
        const cfg = this.config;
        return await S3Ops.putS3(cfg.awscfg!, cfg.bucket, key, content, contentType);
    }

    // Get string
    static async getS3Cmd(s3: S3Client, bucket: string, key: string)
    {
        return await s3.send(new GetObjectCommand({
            Bucket: bucket,
            Key: key,
        }));
    }

    static async getS3(awscfg: AWSServiceConfig, bucket: string, key: string)
    {
        const s3 = S3Ops.createS3Client(awscfg);

        return await S3Ops.getS3Cmd(s3, bucket, key);
    }

    @Communicator()
    async getS3Comm(_ctx: CommunicatorContext, key: string)
    {
        const cfg = this.config;
        return (await S3Ops.getS3(cfg.awscfg!, cfg.bucket, key)).Body?.transformToString();
    }

    // Presigned GET key
    static async getS3KeyCmd(s3: S3Client, bucket: string, key: string, expirationSecs: number)
    {
        const getObjectCommand = new GetObjectCommand({
            Bucket: bucket,
            Key: key,
        });

        const presignedUrl = await getSignedUrl(s3, getObjectCommand, { expiresIn: expirationSecs, });
        return presignedUrl;
    }

    static async getS3Key(awscfg: AWSServiceConfig, bucket: string, key: string, expirationSecs: number)
    {
        const s3 = S3Ops.createS3Client(awscfg);

        return await S3Ops.getS3KeyCmd(s3, bucket, key, expirationSecs);
    }

    @Communicator()
    async getS3KeyComm(ctx: CommunicatorContext, key: string, expirationSecs: number = 3600)
    {
        const cfg = this.config;
        return await S3Ops.getS3Key(cfg.awscfg!, cfg.bucket, key, expirationSecs);
    }

    // Presigned post key
    static async postS3KeyCmd(s3: S3Client, bucket: string, key: string, expirationSecs: number,
        contentOptions?: {
            contentType?: string,
            contentLengthMin?: number,
            contentLengthMax?: number,
        }
    ) : Promise<PresignedPost>
    {
        const postPresigned = await createPresignedPost(
            s3,
            {
                Conditions: [
                    ["content-length-range", contentOptions?.contentLengthMin ?? 1,
                                            contentOptions?.contentLengthMax ?? 10000000], // 10MB
                ],
                Bucket: bucket,
                Key: key,
                Expires: expirationSecs,
                Fields: {
                    'Content-Type': contentOptions?.contentType ?? '*',
                }
            }
        );
        return {url: postPresigned.url, fields: postPresigned.fields};
    }

    static async postS3Key(awscfg: AWSServiceConfig, bucket: string, key: string, expirationSecs: number,
        contentOptions?: {
            contentType?: string,
            contentLengthMin?: number,
            contentLengthMax?: number,
        })
    {
        const s3 = S3Ops.createS3Client(awscfg);

        return await S3Ops.postS3KeyCmd(s3, bucket, key, expirationSecs, contentOptions);
    }

    @Communicator()
    async postS3KeyComm(ctx: CommunicatorContext, key: string, expirationSecs: number = 1200,
        @ArgOptional contentOptions?: {
            contentType?: string,
            contentLengthMin?: number,
            contentLengthMax?: number,
        })
    {
        try {
            const cfg = this.config;
            return await S3Ops.postS3Key(cfg.awscfg!, cfg.bucket, key, expirationSecs, contentOptions);
        }
        catch (e) {
            ctx.logger.error(e);
            throw new DBOSError(`Unable to presign post: ${(e as Error).message}`, 500);
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
    async saveStringToFile(ctx: WorkflowContext, fileDetails: FileRecord, content: string, @ArgOptional contentType = 'text/plain') 
    {
        // Running this as a communicator could possibly be skipped... but only for efficiency
        try {
            await ctx.invoke(this).putS3Comm(fileDetails.key, content, contentType);
        }
        catch (e) {
            await ctx.invoke(this).deleteS3Comm(fileDetails.key);
            throw e;
        }
    
        await this.config.s3Callbacks?.newActiveFile(ctx, fileDetails);
        return fileDetails;
    }

    //  App code reads a file out of S3
    @Workflow()
    async readStringFromFile(ctx: WorkflowContext, fileDetails: FileRecord)
    {
        const txt = await ctx.invoke(this).getS3Comm(fileDetails.key);
        return txt;
    }

    //  App code deletes a file out of S3
    //     Do the table write
    //     Do the S3 op
    @Workflow()
    async deleteFile(ctx: WorkflowContext, fileDetails: FileRecord)
    {
        await this.config.s3Callbacks?.fileDeleted(ctx, fileDetails);
        return await ctx.invoke(this).deleteS3Comm(fileDetails.key);
    }

    ////////////
    // Workflows where client goes direct to S3 (slightly more complicated)
    ////////////

    // There are cases where we don't want to store the data in the DB
    //   Especially end-user uploads, or cases where the file is accessed by outside systems

    //  Presigned D/L for end user
    @Workflow()
    async getFileReadURL(ctx: WorkflowContext, fileDetails: FileRecord, @ArgOptional expirationSec = 3600) : Promise<string>
    {
        return await ctx.invoke(this).getS3KeyComm(fileDetails.key, expirationSec);
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
    async writeFileViaURL(ctx: WorkflowContext, fileDetails: FileRecord,
        @ArgOptional expirationSec = 3600,
        @ArgOptional contentOptions?: {
            contentType?: string,
            contentLengthMin?: number,
            contentLengthMax?: number,
        }
    )
    {
        await this.config.s3Callbacks?.newPendingFile(ctx, fileDetails);

        const upkey = await ctx.invoke(this).postS3KeyComm(fileDetails.key, expirationSec, contentOptions);
        await ctx.setEvent<PresignedPost>("uploadkey", upkey);

        try {
            await ctx.recv("uploadfinish", expirationSec + 60); // 1 minute extra?

            // TODO: Validate the file
            await this.config.s3Callbacks?.fileActivated(ctx, fileDetails);
        }
        catch (e) {
            try {
                const _cwfh = await ctx.startWorkflow(this).deleteFile(fileDetails);
                // No reason to await result
            }
            catch (e2) {
                ctx.logger.debug(e2);
            }
            throw e;
        }

        return fileDetails;
    }
}
