import { type PresignedPost } from '@aws-sdk/s3-presigned-post';

import { DBOS, WorkflowConfig } from '@dbos-inc/dbos-sdk';

export interface FileRecord {
  key: string;
}

export interface S3WorkflowCallbacks<R extends FileRecord, Options = unknown> {
  // Database operations (these should be transactions)
  newActiveFile: (rec: R) => Promise<unknown>;
  newPendingFile: (rec: R) => Promise<unknown>;
  fileActivated: (rec: R) => Promise<unknown>;
  fileDeleted: (rec: R) => Promise<unknown>;

  // S3 interaction options, these will be run as steps
  putS3Contents: (rec: R, content: string, options?: Options) => Promise<unknown>;
  createPresignedPost: (rec: R, timeout?: number, options?: Options) => Promise<PresignedPost>;
  validateS3Upload?: (rec: R) => Promise<void>;
  deleteS3Object: (rec: R) => Promise<unknown>;
}

export function registerS3DeleteWorkflow<R extends FileRecord, Options = unknown>(
  options: {
    name?: string;
    ctorOrProto?: object;
    className?: string;
    config?: WorkflowConfig;
  },
  callbacks: S3WorkflowCallbacks<R, Options>,
) {
  return DBOS.registerWorkflow(async (fileDetails: R) => {
    await callbacks.fileDeleted(fileDetails);
    return await DBOS.runStep(
      async () => {
        return callbacks.deleteS3Object(fileDetails);
      },
      { name: 'deleteS3Object' },
    );
  }, options);
}

export function registerS3UploadWorkflow<R extends FileRecord, Options = unknown>(
  options: {
    name?: string;
    ctorOrProto?: object;
    className?: string;
    config?: WorkflowConfig;
  },
  callbacks: S3WorkflowCallbacks<R, Options>,
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
  }, options);
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
