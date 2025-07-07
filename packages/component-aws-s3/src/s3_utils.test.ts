import { FileRecord, DBOS_S3, S3WorkflowCallbacks, registerS3PresignedUploadWorkflow } from './s3_utils';
import { DBOS } from '@dbos-inc/dbos-sdk';

import { S3Client, DeleteObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { createPresignedPost, PresignedPost } from '@aws-sdk/s3-presigned-post';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

import FormData from 'form-data';
import axios, { AxiosResponse } from 'axios';
import { Readable } from 'stream';
import * as fs from 'fs';
import { randomUUID } from 'node:crypto';

enum FileStatus {
  PENDING = 'Pending',
  RECEIVED = 'Received',
  ACTIVE = 'Active',
}

interface FileDetails {
  user_id: string;
  file_type: string;
  file_name: string;
  file_id?: string;
  file_status?: string;
  file_time?: number;
}
interface UserFile extends FileDetails, FileRecord {
  user_id: string;
  file_type: string;
  file_name: string;
  file_id: string;
  file_status: string;
  file_time: number;
}

class TestUserFileTable {
  //////////
  //// Database table
  //////////

  // Pick a file ID
  @DBOS.step()
  static chooseFileRecord(details: FileDetails): Promise<UserFile> {
    const rec: UserFile = {
      user_id: details.user_id,
      file_status: FileStatus.PENDING,
      file_type: details.file_type,
      file_id: randomUUID(),
      file_name: details.file_name,
      file_time: new Date().getTime(),
      key: '',
    };
    rec.key = TestUserFileTable.createS3Key(rec);
    return Promise.resolve(rec);
  }

  static createS3Key(rec: UserFile) {
    const key = `${rec.file_type}/${rec.user_id}/${rec.file_id}/${rec.file_time}`;
    return key;
  }

  static toFileDetails(rec: UserFile) {
    return {
      user_id: rec.user_id,
      file_type: rec.file_type,
      file_name: rec.file_name,
      file_id: rec.file_id,
      file_status: rec.file_status,
      file_time: rec.file_time,
    };
  }

  // File table DML operations
  // Whole record is known
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
  // Delete when part of record is known
  @DBOS.transaction()
  static async deleteFileRecordById(file_id: string) {
    await DBOS.knexClient<FileDetails>('user_files').delete().where({ file_id });
  }

  // Queries
  @DBOS.transaction({ readOnly: true })
  static async lookUpByFields(fields: FileDetails) {
    const rv = await DBOS.knexClient<FileDetails>('user_files')
      .select()
      .where({ ...fields, file_status: FileStatus.ACTIVE })
      .orderBy('file_time', 'desc')
      .first();
    return rv ? [rv] : [];
  }
  @DBOS.transaction({ readOnly: true })
  static async lookUpByName(user_id: string, file_type: string, file_name: string) {
    const rv = await DBOS.knexClient<FileDetails>('user_files')
      .select()
      .where({ user_id, file_type, file_name, file_status: FileStatus.ACTIVE })
      .orderBy('file_time', 'desc')
      .first();
    return rv ? [rv] : [];
  }
  @DBOS.transaction({ readOnly: true })
  static async lookUpByType(user_id: string, file_type: string) {
    const rv = await DBOS.knexClient<FileDetails>('user_files')
      .select()
      .where({ user_id, file_type, file_status: FileStatus.ACTIVE });
    return rv;
  }
  @DBOS.transaction({ readOnly: true })
  static async lookUpByUser(user_id: string) {
    const rv = await DBOS.knexClient<FileDetails>('user_files')
      .select()
      .where({ user_id, file_status: FileStatus.ACTIVE });
    return rv;
  }
}

const s3bucket = process.env['S3_BUCKET'];
const s3region = process.env['AWS_REGION'];
const s3accessKey = process.env['AWS_ACCESS_KEY_ID'];
const s3accessSecret = process.env['AWS_SECRET_ACCESS_KEY'];

let s3client: S3Client | undefined = undefined;

interface Opts {
  contentType?: string;
}

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

  validateS3Upload: undefined,
  deleteS3Object: async (rec: UserFile) => {
    await s3client?.send(
      new DeleteObjectCommand({
        Bucket: s3bucket,
        Key: rec.key,
      }),
    );
  },
};

export const uploadWF = registerS3PresignedUploadWorkflow({ name: 'uploadWF' }, s3callback);

describe('ses-tests', () => {
  let s3IsAvailable = true;
  let s3Cfg: DBOS_S3 | undefined = undefined;

  beforeAll(async () => {
    // Check if S3 is available and update app config, skip the test if it's not
    if (!s3region || !s3bucket || !s3accessKey || !s3accessSecret) {
      s3IsAvailable = false;
      console.log(
        'S3 Test is not configured.  To run, set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET',
      );
    } else {
      s3client = new S3Client({
        region: s3region,
        credentials: {
          accessKeyId: s3accessKey,
          secretAccessKey: s3accessSecret,
        },
      });

      s3Cfg = new DBOS_S3('default', {
        awscfgname: 'aws_config',
        bucket: '',
        s3Callbacks: {
          newActiveFile: async (frec: unknown) => {
            const rec = frec as UserFile;
            rec.file_status = FileStatus.ACTIVE;
            return await TestUserFileTable.insertFileRecord(rec);
          },
          newPendingFile: async (frec: unknown) => {
            const rec = frec as UserFile;
            rec.file_status = FileStatus.PENDING;
            return await TestUserFileTable.insertFileRecord(rec);
          },
          fileActivated: async (frec: unknown) => {
            const rec = frec as UserFile;
            rec.file_status = FileStatus.ACTIVE;
            return await TestUserFileTable.updateFileRecord(rec);
          },
          fileDeleted: async (frec: unknown) => {
            const rec = frec as UserFile;
            return await TestUserFileTable.deleteFileRecordById(rec.file_id);
          },
        },
      });
      await DBOS.launch();
      s3Cfg.config.bucket = DBOS.getConfig<string>('s3_bucket', 's3bucket');
    }
  });

  afterAll(async () => {
    if (s3IsAvailable) {
      await DBOS.shutdown();
    }
  }, 10000);

  test('s3-simple-wfs', async () => {
    if (!s3IsAvailable) {
      console.log('S3 unavailable, skipping S3 tests');
      return;
    }

    const userid = randomUUID();

    // The simple workflows that will be performed are to:
    //   Put file contents into DBOS (w/ table index)
    const myFile: FileDetails = { user_id: userid, file_type: 'text', file_name: 'mytextfile.txt' };
    const myFileRec = await TestUserFileTable.chooseFileRecord(myFile);
    await s3Cfg!.saveStringToFile(myFileRec, 'This is my file');

    // Get the file contents out of DBOS (using the table index)
    const mytxt = await getS3KeyContents(myFileRec.key);
    expect(mytxt).toBe('This is my file');

    // Delete the file contents out of DBOS (using the table index)
    const dfhandle = await s3Cfg!.deleteFile(myFileRec);
    expect(dfhandle).toBeDefined();
  }, 10000);

  test('s3-complex-wfs', async () => {
    if (!s3IsAvailable) {
      console.log('S3 unavailable, skipping S3 tests');
      return;
    }

    // The complex workflows that will be performed are to:
    //  Put the file contents into DBOS with a presigned post
    const userid = randomUUID();

    // The simple workflows that will be performed are to:
    //   Put file contents into DBOS (w/ table index)
    const myFile: FileDetails = { user_id: userid, file_type: 'text', file_name: 'mytextfile.txt' };
    const myFileRec = await TestUserFileTable.chooseFileRecord(myFile);
    const wfhandle = await DBOS.startWorkflow(uploadWF)(myFileRec, 60, { contentType: 'text/plain' });
    //    Get the presigned post
    const ppost = await DBOS.getEvent<PresignedPost>(wfhandle.workflowID, 'uploadkey');
    //    Upload to the URL
    try {
      const res = await uploadToS3(ppost!, './src/s3_utils.test.ts');
      expect(res.status.toString()[0]).toBe('2');
    } catch (e) {
      // You do NOT want to accidentally serialize an AxiosError - they don't!
      console.log('Caught something awful!', e);
      expect(e).toBeUndefined();
    }
    //    Notify WF
    await DBOS.send<boolean>(wfhandle.workflowID, true, 'uploadfinish');

    //    Wait for WF complete
    const _myFileRecord = await wfhandle.getResult();

    // Get the file out of DBOS (using a signed URL)
    const myurl = await getS3KeyUrl(myFileRec.key, 60);
    expect(myurl).not.toBeNull();
    // Get the file contents out of S3
    await downloadFromS3(myurl, './deleteme.xxx');
    expect(fs.existsSync('./deleteme.xxx')).toBeTruthy();
    fs.rmSync('./deleteme.xxx');

    // Delete the file contents out of DBOS (No different than above)
    // Delete the file contents out of DBOS (using the table index)
    const dfhandle = await s3Cfg!.deleteFile(myFileRec);
    expect(dfhandle).toBeDefined();
  }, 10000);

  async function getS3KeyContents(key: string) {
    return (
      await s3client!.send(
        new GetObjectCommand({
          Bucket: s3bucket!,
          Key: key,
        }),
      )
    ).Body?.transformToString();
  }

  async function getS3KeyUrl(key: string, expirationSecs: number) {
    const getObjectCommand = new GetObjectCommand({
      Bucket: s3bucket!,
      Key: key,
    });

    const presignedUrl = await getSignedUrl(s3client!, getObjectCommand, { expiresIn: expirationSecs });
    return presignedUrl;
  }

  async function uploadToS3(presignedPostData: PresignedPost, filePath: string) {
    const formData = new FormData();

    // Append all the fields from the presigned post data
    Object.keys(presignedPostData.fields).forEach((key) => {
      formData.append(key, presignedPostData.fields[key]);
    });

    // Append the file you want to upload
    const fileStream = fs.createReadStream(filePath);
    formData.append('file', fileStream);

    return await axios.post(presignedPostData.url, formData);
  }

  async function downloadFromS3(presignedGetUrl: string, outputPath: string) {
    const response: AxiosResponse<Readable> = await axios.get(presignedGetUrl, {
      responseType: 'stream', // Important to handle large files
    });

    // Use a write stream to save the file to the desired path
    const writer = fs.createWriteStream(outputPath);
    response.data.pipe(writer);

    return new Promise<void>((resolve, reject) => {
      writer.on('finish', resolve);
      writer.on('error', reject);
    });
  }
});
