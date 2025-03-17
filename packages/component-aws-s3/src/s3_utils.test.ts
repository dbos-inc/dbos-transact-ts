import { FileRecord, DBOS_S3 } from './s3_utils';
import { DBOS } from '@dbos-inc/dbos-sdk';

import FormData from 'form-data';
import axios, { AxiosResponse } from 'axios';
import { PresignedPost } from '@aws-sdk/s3-presigned-post';
import { Readable } from 'stream';
import * as fs from 'fs';
import { v4 as uuidv4 } from 'uuid';

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
      file_id: uuidv4(),
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

describe('ses-tests', () => {
  let s3IsAvailable = true;
  let s3Cfg: DBOS_S3 | undefined = undefined;

  beforeAll(async () => {
    // Check if S3 is available and update app config, skip the test if it's not
    if (!process.env['AWS_REGION'] || !process.env['S3_BUCKET']) {
      s3IsAvailable = false;
      console.log(
        'S3 Test is not configured.  To run, set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET',
      );
    } else {
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
  }, 20000);

  test('s3-basic-ops', async () => {
    if (!s3IsAvailable) {
      console.log('S3 unavailable, skipping S3 tests');
      return;
    }

    const fn = `filepath/FN_${new Date().toISOString()}`;
    const putres = await s3Cfg!.put(fn, 'Test string from DBOS');
    expect(putres).toBeDefined();

    const getres = await s3Cfg!.get(fn);
    expect(getres).toBeDefined();
    expect(getres).toBe('Test string from DBOS');

    const delres = await s3Cfg!.delete(fn);
    expect(delres).toBeDefined();
  });

  test('s3-presgined-ops', async () => {
    if (!s3IsAvailable) {
      console.log('S3 unavailable, skipping S3 tests');
      return;
    }

    const fn = `presigned_filepath/FN_${new Date().toISOString()}`;

    const postres = await s3Cfg!.createPresignedPost(fn, 30, { contentType: 'text/plain' });
    expect(postres).toBeDefined();
    try {
      const res = await uploadToS3(postres, './src/s3_utils.test.ts');
      expect(res.status.toString()[0]).toBe('2');
    } catch (e) {
      // You do NOT want to accidentally serialize an AxiosError - they don't!
      console.log('Caught something awful!', e);
      expect(e).toBeUndefined();
    }

    // Make a fetch request to test it...
    const geturl = await s3Cfg!.presignedGetURL(fn, 30);
    await downloadFromS3(geturl, './deleteme.xxx');
    expect(fs.existsSync('./deleteme.xxx')).toBeTruthy();
    fs.rmSync('./deleteme.xxx');

    // Delete it
    const delres = await s3Cfg!.delete(fn);
    expect(delres).toBeDefined();
  });

  test('s3-simple-wfs', async () => {
    if (!s3IsAvailable) {
      console.log('S3 unavailable, skipping S3 tests');
      return;
    }

    const userid = uuidv4();

    // The simple workflows that will be performed are to:
    //   Put file contents into DBOS (w/ table index)
    const myFile: FileDetails = { user_id: userid, file_type: 'text', file_name: 'mytextfile.txt' };
    const myFileRec = await TestUserFileTable.chooseFileRecord(myFile);
    await s3Cfg!.saveStringToFile(myFileRec, 'This is my file');

    // Get the file contents out of DBOS (using the table index)
    const mytxt = await s3Cfg!.readStringFromFile(myFileRec);
    expect(mytxt).toBe('This is my file');

    // Delete the file contents out of DBOS (using the table index)
    const dfhandle = await s3Cfg!.deleteFile(myFileRec);
    expect(dfhandle).toBeDefined();
  });

  test('s3-complex-wfs', async () => {
    if (!s3IsAvailable) {
      console.log('S3 unavailable, skipping S3 tests');
      return;
    }

    // The complex workflows that will be performed are to:
    //  Put the file contents into DBOS with a presigned post
    const userid = uuidv4();

    // The simple workflows that will be performed are to:
    //   Put file contents into DBOS (w/ table index)
    const myFile: FileDetails = { user_id: userid, file_type: 'text', file_name: 'mytextfile.txt' };
    const myFileRec = await TestUserFileTable.chooseFileRecord(myFile);
    const wfhandle = await DBOS.startWorkflow(s3Cfg!).writeFileViaURL(myFileRec, 60, { contentType: 'text/plain' });
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
    const myurl = await s3Cfg!.getFileReadURL(myFileRec);
    expect(myurl).not.toBeNull();
    // Get the file contents out of S3
    await downloadFromS3(myurl, './deleteme.xxx');
    expect(fs.existsSync('./deleteme.xxx')).toBeTruthy();
    fs.rmSync('./deleteme.xxx');

    // Delete the file contents out of DBOS (No different than above)
    // Delete the file contents out of DBOS (using the table index)
    const dfhandle = await s3Cfg!.deleteFile(myFileRec);
    expect(dfhandle).toBeDefined();
  });

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

    return new Promise((resolve, reject) => {
      writer.on('finish', resolve);
      writer.on('error', reject);
    });
  }
});
