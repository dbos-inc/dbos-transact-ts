import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { createPresignedPost, PresignedPost } from '@aws-sdk/s3-presigned-post';

import {
    ArgOptional,
    Communicator, CommunicatorContext,
    Configurable,
    InitContext,
    Workflow,
    WorkflowContext
} from '@dbos-inc/dbos-sdk';
import { AWSServiceConfig, getAWSConfigForService, getAWSConfigs } from '@dbos-inc/aws-config';
import { DBOSError } from '@dbos-inc/dbos-sdk/dist/src/error';

export interface FileRecord {
    key: string;
}

export interface S3Config{
    awscfgname?: string,
    awscfg?: AWSServiceConfig,
    bucket: string,
    s3Callbacks: {
        newActiveFile: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
        newPendingFile: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
        fileActivated: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
        fileDeleted: (ctx: WorkflowContext, rec: FileRecord) => Promise<unknown>;
    }
}

@Configurable()
export class S3Ops {
    //////////
    // S3 Configuration
    //////////

    static AWS_S3_CONFIGURATIONS = 'aws_s3_configurations';

    static async initConfiguration(ctx: InitContext, config: S3Config) {
        // Get the config and call the validation
        getAWSConfigs(ctx, S3Ops.AWS_S3_CONFIGURATIONS);
        if (!config.awscfg) {
            config.awscfg = getAWSConfigForService(ctx, this.AWS_S3_CONFIGURATIONS, config.awscfgname ?? "");
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
    static async deleteS3Comm(ctx: CommunicatorContext, key: string)
    {
        const cfg = ctx.getConfiguredClass(S3Ops).config;
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
    static async putS3Comm(ctx: CommunicatorContext, key: string, content: string, @ArgOptional contentType: string = 'text/plain')
    {
        const cfg = ctx.getConfiguredClass(S3Ops).config;
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
    static async getS3Comm(ctx: CommunicatorContext, key: string)
    {
        const cfg = ctx.getConfiguredClass(S3Ops).config;
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
    static async getS3KeyComm(ctx: CommunicatorContext, key: string, expirationSecs: number = 3600)
    {
        const cfg = ctx.getConfiguredClass(S3Ops).config;
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
    static async postS3KeyComm(ctx: CommunicatorContext, key: string, expirationSecs: number = 1200,
        @ArgOptional contentOptions?: {
            contentType?: string,
            contentLengthMin?: number,
            contentLengthMax?: number,
        })
    {
        try {
            const cfg = ctx.getConfiguredClass(S3Ops).config;
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
    //     Pick a file name
    //     Do the S3 op
    //     If it succeeds, write in table
    //     If it fails drop the partial file (if any) from S3
    // Note this will ALL get logged in the DB as a workflow parameter (and a communicator parameter) so better not be big!
    @Workflow()
    static async saveStringToFile(ctx: WorkflowContext, fileDetails: FileRecord, content: string, @ArgOptional contentType = 'text/plain') 
    {
        const cfc = ctx.getConfiguredClass(S3Ops);

        // Running this as a communicator could possibly be skipped... but only for efficiency
        try {
            await ctx.invoke(cfc).putS3Comm(fileDetails.key, content, contentType);
        }
        catch (e) {
            await ctx.invoke(cfc).deleteS3Comm(fileDetails.key);
            throw e;
        }
    
        await cfc.config.s3Callbacks.newActiveFile(ctx, fileDetails);
        return fileDetails;
    }

    //  App code reads a file out of S3
    //  Just a routine, hardly worthy of a workflow
    //    Does the DB query
    //    Does the S3 read
    @Workflow()
    static async readStringFromFile(ctx: WorkflowContext, fileDetails: FileRecord)
    {
        const cfc = ctx.getConfiguredClass(S3Ops);
        const txt = await ctx.invoke(cfc).getS3Comm(fileDetails.key);
        return txt;
    }

    //  App code deletes a file out of S3
    //   One-shot workflow
    //     Do the table write
    //     Do the S3 op
    //   Wrapper function to wait
    @Workflow()
    static async deleteFile(ctx: WorkflowContext, fileDetails: FileRecord)
    {
        const cfc = ctx.getConfiguredClass(S3Ops);
        await cfc.config.s3Callbacks.fileDeleted(ctx, fileDetails);
        return await ctx.invoke(cfc).deleteS3Comm(fileDetails.key);
    }

    ////////////
    // Workflows where client goes direct to S3 (slightly more complicated)
    ////////////

    // There are cases where we don't want to store the data in the DB
    //   Especially end-user uploads, or cases where the file is accessed by outside systems

    //  Presigned D/L for end user
    //    Hardly warrants a full workflow
    @Workflow()
    static async getFileReadURL(ctx: WorkflowContext, fileDetails: FileRecord, @ArgOptional expirationSec = 3600) : Promise<string>
    {
        const cfc = ctx.getConfiguredClass(S3Ops);
        return await ctx.invoke(cfc).getS3KeyComm(fileDetails.key, expirationSec);
    }

    //  Presigned U/L for end user
    //    A workflow that creates a presigned post
    //    Sets that back for the caller
    //    Waits for a completion
    //    If it gets it, adds the DB entry
    //      Should it poll for upload in case nobody tells it?  (Do a HEAD request w/ exponential backoff, listen to SQS)
    //      The poll will end significantly after the S3 post URL expires, so we'll know
    //    This will support an OAOO key on the workflow, no problem.
    //      We won't start that completion checker more than once
    //      If you ask again, you get the same presigned post URL
    @Workflow()
    static async writeFileViaURL(ctx: WorkflowContext, fileDetails: FileRecord,
        @ArgOptional expirationSec = 3600,
        @ArgOptional contentOptions?: {
            contentType?: string,
            contentLengthMin?: number,
            contentLengthMax?: number,
        }
    )
    {
        const cfc = ctx.getConfiguredClass(S3Ops);

        await cfc.config.s3Callbacks.newPendingFile(ctx, fileDetails);

        const upkey = await ctx.invoke(cfc).postS3KeyComm(fileDetails.key, expirationSec, contentOptions);
        await ctx.setEvent<PresignedPost>("uploadkey", upkey);

        try {
            await ctx.recv("uploadfinish", expirationSec + 60); // 1 minute extra?

            // TODO: Validate the file
            await cfc.config.s3Callbacks.fileActivated(ctx, fileDetails);
        }
        catch (e) {
            try {
                const _cwfh = await ctx.startChildWorkflow(cfc, S3Ops.deleteFile, fileDetails);
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
