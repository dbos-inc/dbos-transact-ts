import { S3Ops } from "./s3_utils";
export { S3Ops };
import {
  TestingRuntime, createTestingRuntime,
  ConfiguredClassType, initClassConfiguration,
  Communicator, CommunicatorContext,
  Transaction, TransactionContext,
  WorkflowContext,
} from "@dbos-inc/dbos-sdk";

import { Knex } from 'knex';
import FormData from 'form-data';
import axios, { AxiosResponse } from 'axios';
import { PresignedPost } from "@aws-sdk/s3-presigned-post";
import { Readable } from 'stream';
import * as fs from 'fs';
import { v4 as uuidv4 } from 'uuid';

enum FileStatus
{
    PENDING  = 'Pending',
    RECEIVED = 'Received',
    ACTIVE   = 'Active',
}

interface FileDetails {
  user_id: string;
  file_type: string;
  file_name: string;
}
interface UserFile {
  file_id: string,
  user_id: string,
  file_status: string,
  file_type: string,
  file_time: number,
  file_name: string,
}

type KnexTransactionContext = TransactionContext<Knex>;

class TestUserFileTable {
  //////////
  //// Database table
  //////////

  // Pick a file ID
  @Communicator()
  static chooseFileRecord(_ctx: CommunicatorContext, details: FileDetails): Promise<UserFile> {
      return Promise.resolve({
          user_id: details.user_id,
          file_status: FileStatus.PENDING,
          file_type: details.file_type,
          file_id: uuidv4(),
          file_name: details.file_name,
          file_time: new Date().getTime(),
      });
  }

  static createS3Key(rec: UserFile) {
      const key = `${rec.file_type}/${rec.user_id}/${rec.file_id}/${rec.file_time}`;
      return key;
  }
  
  // File table DML operations
  // Whole record is known
  @Transaction()
  static async insertFileRecord(ctx: KnexTransactionContext, rec: UserFile) {
      await ctx.client<UserFile>('user_files').insert(rec);
  }
  @Transaction()
  static async updateFileRecord(ctx: KnexTransactionContext, rec: UserFile) {
      await ctx.client<UserFile>('user_files').update(rec).where({file_id: rec.file_id});
  }
  @Transaction()
  static async deleteFileRecord(ctx: KnexTransactionContext, rec: UserFile) {
      await ctx.client<UserFile>('user_files').delete().where({file_id: rec.file_id});
  }
  // Delete when part of record is known
  @Transaction()
  static async deleteFileRecordById(ctx: KnexTransactionContext, file_id: string) {
      await ctx.client<UserFile>('user_files').delete().where({file_id});
  }

  // Queries
  @Transaction({readOnly: true})
  static async lookUpByFields(ctx: KnexTransactionContext, fields: FileDetails) {
      const rv = await ctx.client<UserFile>('user_files').select().where({...fields, file_status: FileStatus.ACTIVE}).orderBy('file_time', 'desc').first();
      return rv ? [rv] : [];
  }
  @Transaction({readOnly: true})
  static async lookUpByName(ctx: KnexTransactionContext, user_id: string, file_type: string, file_name: string) {
      const rv = await ctx.client<UserFile>('user_files').select().where({user_id, file_type, file_name, file_status: FileStatus.ACTIVE}).orderBy('file_time', 'desc').first();
      return rv ? [rv] : [];
  }
  @Transaction({readOnly: true})
  static async lookUpByType(ctx: KnexTransactionContext, user_id: string, file_type: string) {
      const rv = await ctx.client<UserFile>('user_files').select().where({user_id, file_type, file_status: FileStatus.ACTIVE});
      return rv;
  }
  @Transaction({readOnly: true})
  static async lookUpByUser(ctx: KnexTransactionContext, user_id: string) {
      const rv = await ctx.client<UserFile>('user_files').select().where({user_id, file_status: FileStatus.ACTIVE});
      return rv;
  }
}

describe("ses-tests", () => {
  let testRuntime: TestingRuntime | undefined = undefined;
  let s3IsAvailable = true;
  let s3Cfg : ConfiguredClassType<typeof S3Ops> | undefined = undefined;

  beforeAll(() => {
    // Check if SES is available and update app config, skip the test if it's not
    if (!process.env['AWS_REGION'] || !process.env['S3_BUCKET'] ) {
      s3IsAvailable = false;
    }
    else {
      s3Cfg = initClassConfiguration(S3Ops, 'default', {
        awscfgname: 'aws_config',
        bucket: "",
        tableOps: {
          createS3Key: (f) => {return TestUserFileTable.createS3Key(f as UserFile);},
          createFileRecord: async (ctx: WorkflowContext, fdt: unknown) => {
            const details = fdt as FileDetails;
            return await ctx.invoke(TestUserFileTable).chooseFileRecord(details);
          },
          lookUpFileRecord: async (ctx: WorkflowContext, fdt: unknown) => {
            const details = fdt as FileDetails;
            const res = await ctx.invoke(TestUserFileTable).lookUpByFields(details);
            if (res.length < 1) {
              throw new Error(`File not Found: ${JSON.stringify(fdt)}`);
            }
            if (res.length > 1) {
              throw new Error(`File not unique: ${JSON.stringify(fdt)}`);
            }
            return res[0];
          },
          insertActiveFileRecord: async (ctx: WorkflowContext, frec: unknown) => {
            const rec = frec as UserFile;
            rec.file_status = FileStatus.ACTIVE;
            return await ctx.invoke(TestUserFileTable).insertFileRecord(rec);
          },
          insertPendingFileRecord: async (ctx: WorkflowContext, frec: unknown) => {
            const rec = frec as UserFile;
            rec.file_status = FileStatus.PENDING;
            return await ctx.invoke(TestUserFileTable).insertFileRecord(rec);
          },
          activateFileRecord: async (ctx: WorkflowContext, frec: unknown) => {
            const rec = frec as UserFile;
            rec.file_status = FileStatus.ACTIVE;
            return await ctx.invoke(TestUserFileTable).updateFileRecord(rec);
          },
          deleteFileRecord: async (ctx: WorkflowContext, frec: unknown) => {
            const rec = frec as UserFile;
            return await ctx.invoke(TestUserFileTable).deleteFileRecordById(rec.file_id);
          }
        }
      });
    }
  });

  beforeEach(async () => {
    if (s3IsAvailable) {
      testRuntime = await createTestingRuntime([S3Ops, TestUserFileTable], 'dbos-config.yaml');
      s3Cfg!.arg.bucket = testRuntime.getConfig<string>('s3_bucket', 's3bucket');
    }
    else {
      console.log("S3 Test is not configured.  To run, set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET");
    }
  });

  afterEach(async () => {
    if (s3IsAvailable) {
      await testRuntime?.destroy();
    }
  }, 10000);

  test("s3-basic-ops", async () => {
    if (!s3IsAvailable || !testRuntime) {
      console.log("S3 unavailable, skipping SES tests");
      return;
    }

    const fn = `filepath/FN_${new Date().toISOString()}`;
    const putres = await testRuntime.invokeOnConfig(s3Cfg!).putS3Comm(fn, "Test string from DBOS");
    expect(putres).toBeDefined();

    const getres = await testRuntime.invokeOnConfig(s3Cfg!).getS3Comm(fn);
    expect(getres).toBeDefined();
    expect(getres).toBe('Test string from DBOS');

    const delres = await testRuntime.invokeOnConfig(s3Cfg!).deleteS3Comm(fn);
    expect(delres).toBeDefined();
  });

  test("s3-presgined-ops", async () => {
    if (!s3IsAvailable || !testRuntime) {
      console.log("S3 unavailable, skipping SES tests");
      return;
    }
  
    const fn = `presigned_filepath/FN_${new Date().toISOString()}`;

    const postres = await testRuntime.invokeOnConfig(s3Cfg!).postS3KeyComm(fn, 30, {contentType: 'text/plain'});
    expect(postres).toBeDefined();
    try {
        const res = await uploadToS3(postres, './src/s3_utils.test.ts');
        expect(res.status.toString()[0]).toBe('2');
    }
    catch(e) {
        // You do NOT want to accidentally serialize an AxiosError - they don't!
        console.log("Caught something awful!", e);
        expect(e).toBeUndefined();
    }

    // Make a fetch request to test it...
    const geturl = await testRuntime.invokeOnConfig(s3Cfg!).getS3KeyComm(fn, 30);
    await downloadFromS3(geturl, './deleteme.xxx');
    expect(fs.existsSync('./deleteme.xxx')).toBeTruthy();
    fs.rmSync('./deleteme.xxx');

    // Delete it
    const delres = await testRuntime.invokeOnConfig(s3Cfg!).deleteS3Comm(fn);
    expect(delres).toBeDefined();
  });

  test("s3-simple-wfs", async () => {
    if (!s3IsAvailable || !testRuntime) {
      console.log("S3 unavailable, skipping SES tests");
      return;
    }

    const userid = uuidv4();

    // The simple workflows that will be performed are to:
    //   Put file contents into DBOS (w/ table index)
    const myFile : FileDetails = {user_id: userid, file_type: 'text', file_name: 'mytextfile.txt'};
    const _myFileRecord = await testRuntime.invokeWorkflowOnConfig(s3Cfg!).saveStringToFile(
      myFile, 'This is my file'
    );

    // Get the file contents out of DBOS (using the table index)
    const mytxt = await testRuntime.invokeWorkflowOnConfig(s3Cfg!).readStringFromFile(
      myFile
    );
    expect(mytxt).toBe('This is my file');

    // Delete the file contents out of DBOS (using the table index)
    const dfhandle = await testRuntime.invokeWorkflowOnConfig(s3Cfg!).deleteFile(myFile);
    expect(dfhandle).toBeDefined();
  });

  test("s3-complex-wfs", async () => {
    if (!s3IsAvailable || !testRuntime) {
      console.log("S3 unavailable, skipping SES tests");
      return;
    }

    // The complex workflows that will be performed are to:
    //  Put the file contents into DBOS with a presigned post
    const userid = uuidv4();

    // The simple workflows that will be performed are to:
    //   Put file contents into DBOS (w/ table index)
    const myFile : FileDetails = {user_id: userid, file_type: 'text', file_name: 'mytextfile.txt'};
    const wfhandle = await testRuntime.startWorkflowOnConfig(s3Cfg!).writeFileViaURL(myFile, 60, {contentType: 'text/plain'});
    //    Get the presigned post
    const ppost = await testRuntime.getEvent<PresignedPost>(wfhandle.getWorkflowUUID(), "uploadkey");
    //    Upload to the URL
    try {
        const res = await uploadToS3(ppost!, './src/s3_utils.test.ts');
        expect(res.status.toString()[0]).toBe('2');
    }
    catch(e) {
        // You do NOT want to accidentally serialize an AxiosError - they don't!
        console.log("Caught something awful!", e);
        expect(e).toBeUndefined();
    }
    //    Notify WF
    await testRuntime.send<string>(wfhandle.getWorkflowUUID(), "", "uploadfinish");

    //    Wait for WF complete
    const _myFileRecord = await wfhandle.getResult();

    // Get the file out of DBOS (using a signed URL)
    const myurl = await testRuntime.invokeWorkflowOnConfig(s3Cfg!).getFileReadURL(myFile);
    expect (myurl).not.toBeNull();
    // Get the file contents out of S3
    await downloadFromS3(myurl, './deleteme.xxx');
    expect(fs.existsSync('./deleteme.xxx')).toBeTruthy();
    fs.rmSync('./deleteme.xxx');

    // Delete the file contents out of DBOS (No different than above)
    // Delete the file contents out of DBOS (using the table index)
    const dfhandle = await testRuntime.invokeWorkflowOnConfig(s3Cfg!).deleteFile(myFile);
    expect(dfhandle).toBeDefined();
  });

  async function uploadToS3(presignedPostData: PresignedPost, filePath: string) {
        const formData = new FormData();
    
        // Append all the fields from the presigned post data
        Object.keys(presignedPostData.fields).forEach(key => {
            formData.append(key, presignedPostData.fields[key]);
        });
    
        // Append the file you want to upload
        const fileStream = fs.createReadStream(filePath);
        formData.append('file', fileStream);
    
        return await axios.post(presignedPostData.url, formData);
    }
    
    async function downloadFromS3(presignedGetUrl: string, outputPath: string) {
        const response: AxiosResponse<Readable> = await axios.get(presignedGetUrl, {
        responseType: 'stream',  // Important to handle large files
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
