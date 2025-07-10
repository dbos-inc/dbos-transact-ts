import { type PresignedPost } from '@aws-sdk/s3-presigned-post';

import { DBOS, FunctionName, WorkflowConfig } from '@dbos-inc/dbos-sdk';

export interface FileRecord {
  key: string;
}

export interface S3WorkflowCallbacks<R extends FileRecord, Options = unknown> {
  // Database operations (these should be transactions)
  /** Called back when a new, active file is created; should write the file to the database as active */
  newActiveFile: (rec: R) => Promise<unknown>;
  /** Called back when a file might get uploaded; this function may write the file to the database as 'pending' */
  newPendingFile: (rec: R) => Promise<unknown>;
  /** Called back when a pending file becomes active; should write the file to the database as active */
  fileActivated: (rec: R) => Promise<unknown>;
  /** Called back when a file is in the process of getting deleted; should remove the file from the database */
  fileDeleted: (rec: R) => Promise<unknown>;

  // S3 interaction options
  /** Should execute the S3 operation to write contents to rec.key; this will be run as a step */
  putS3Contents: (rec: R, content: string, options?: Options) => Promise<unknown>;
  /** Should execute the S3 operation to create a presigned post for external upload, this will be run as a step */
  createPresignedPost: (rec: R, timeout?: number, options?: Options) => Promise<PresignedPost>;
  /** Optional validation to check if a client S3 upload is valid, before activating in the datbase.  Will run as a step */
  validateS3Upload?: (rec: R) => Promise<void>;
  /** Should execute the S3 operation to delete rec.key; this will be run as a step */
  deleteS3Object: (rec: R) => Promise<unknown>;
}

/**
 * Create a workflow function for deleting S3 objects and removing the DB entry
 * @param callbacks - S3 operation implementation and database recordkeeping transactions
 * @param config - Workflow configuration and arget function name for registration
 */
export function registerS3DeleteWorkflow<R extends FileRecord, Options = unknown>(
  callbacks: S3WorkflowCallbacks<R, Options>,
  config?: WorkflowConfig & FunctionName,
) {
  return DBOS.registerWorkflow(async (fileDetails: R) => {
    await callbacks.fileDeleted(fileDetails);
    return await DBOS.runStep(
      async () => {
        return callbacks.deleteS3Object(fileDetails);
      },
      { name: 'deleteS3Object' },
    );
  }, config);
}

/**
 * Create a workflow function for uploading S3 contents from DBOS
 * @param callbacks - S3 operation implementation and database recordkeeping transactions
 * @param config - Workflow configuration and target function name for registration
 */
export function registerS3UploadWorkflow<R extends FileRecord, Options = unknown>(
  callbacks: S3WorkflowCallbacks<R, Options>,
  config?: WorkflowConfig & FunctionName,
) {
  return DBOS.registerWorkflow(async (fileDetails: R, content: string, objOptions?: Options) => {
    try {
      await DBOS.runStep(
        async () => {
          await callbacks.putS3Contents(fileDetails, content, objOptions);
        },
        { name: 'putS3Contents' },
      );
    } catch (e) {
      try {
        await DBOS.runStep(
          async () => {
            return callbacks.deleteS3Object(fileDetails);
          },
          { name: 'deleteS3Object' },
        );
      } catch (e2) {
        DBOS.logger.debug(e2);
      }
      throw e;
    }

    await callbacks.newActiveFile(fileDetails);
    return fileDetails;
  }, config);
}

/**
 * Create a workflow function for uploading S3 contents externally, via a presigned URL
 * @param callbacks - S3 operation implementation and database recordkeeping transactions
 * @param config - Workflow configuration and target function name for registration
 */
export function registerS3PresignedUploadWorkflow<R extends FileRecord, Options = unknown>(
  callbacks: S3WorkflowCallbacks<R, Options>,
  config?: WorkflowConfig & FunctionName,
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
  }, config);
}
