import { S3Ops } from "./s3_utils";
export { S3Ops };
import { TestingRuntime, createTestingRuntime } from "@dbos-inc/dbos-sdk";

import FormData from 'form-data';
import axios, { AxiosResponse } from 'axios';
import { PresignedPost } from "@aws-sdk/s3-presigned-post";
import { Readable } from 'stream';
import * as fs from 'fs';
import { v4 as uuidv4 } from 'uuid';

describe("ses-tests", () => {
  let testRuntime: TestingRuntime | undefined = undefined;
  let s3IsAvailable = true;

  beforeAll(() => {
    // Check if SES is available and update app config, skip the test if it's not
    if (!process.env['AWS_REGION'] || !process.env['S3_BUCKET'] ) {
      s3IsAvailable = false;
    }
  });

  beforeEach(async () => {
    if (s3IsAvailable) {
      testRuntime = await createTestingRuntime([S3Ops]);
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
    const bucket = testRuntime.getConfig<string>('s3_bucket', 's3bucket');
    const putres = await testRuntime.invoke(S3Ops).putS3Comm(bucket, fn, "Test string from DBOS");
    expect(putres).toBeDefined();

    const getres = await testRuntime.invoke(S3Ops).getS3Comm(bucket, fn);
    expect(getres).toBeDefined();
    expect(getres).toBe('Test string from DBOS');

    const delres = await testRuntime.invoke(S3Ops).deleteS3Comm(bucket, fn);
    expect(delres).toBeDefined();
  });

  test("s3-presgined-ops", async () => {
    if (!s3IsAvailable || !testRuntime) {
      console.log("S3 unavailable, skipping SES tests");
      return;
    }
  
    const fn = `presigned_filepath/FN_${new Date().toISOString()}`;
    const bucket = testRuntime.getConfig<string>('s3_bucket', 's3bucket');

    const postres = await testRuntime.invoke(S3Ops).postS3KeyComm(bucket, fn, 30, {contentType: 'text/plain'});
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
    const geturl = await testRuntime.invoke(S3Ops).getS3KeyComm(bucket, fn, 30);
    await downloadFromS3(geturl, './deleteme.xxx');
    expect(fs.existsSync('./deleteme.xxx')).toBeTruthy();
    fs.rmSync('./deleteme.xxx');

    // Delete it
    const delres = await testRuntime.invoke(S3Ops).deleteS3Comm(bucket, fn);
    expect(delres).toBeDefined();
  });

  test("s3-simple-wfs", async () => {
    if (!s3IsAvailable || !testRuntime) {
      console.log("S3 unavailable, skipping SES tests");
      return;
    }

    const userid = uuidv4();
    const bucket = testRuntime.getConfig<string>('s3_bucket', 's3bucket');

    // The simple workflows that will be performed are to:
    //   Put file contents into DBOS (w/ table index)
    const wfhandle = await testRuntime.invoke(S3Ops).saveStringToFile(userid, 'text', 'mytextfile.txt', bucket, 'This is my file');
    const myFileRecord = await wfhandle.getResult();

    // Get the file contents out of DBOS (using the table index)
    const mthandle = await testRuntime.invoke(S3Ops).readStringFromFile(userid,  'text', myFileRecord.file_name, bucket);
    const mytxt = await mthandle.getResult();
    expect(mytxt).toBe('This is my file');

    // Delete the file contents out of DBOS (using the table index)
    const dfhandle = await testRuntime.invoke(S3Ops).deleteFile(userid, 'text', myFileRecord.file_name, bucket);
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
    const bucket = testRuntime.getConfig<string>('s3_bucket', 's3bucket');

    // The simple workflows that will be performed are to:
    //   Put file contents into DBOS (w/ table index)
    const wfhandle = await testRuntime.invoke(S3Ops).writeFileViaURL(userid, 'text', 'mytextfile.txt', bucket, 60, {contentType: 'text/plain'});
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
    const myFileRecord = await wfhandle.getResult();

    // Get the file out of DBOS (using a signed URL)
    const mthandle = await testRuntime.invoke(S3Ops).getFileReadURL(userid, myFileRecord.file_type, myFileRecord.file_name, bucket);
    const myurl = await mthandle.getResult();
    expect (myurl).not.toBeNull();
    // Get the file contents out of S3
    await downloadFromS3(myurl, './deleteme.xxx');
    expect(fs.existsSync('./deleteme.xxx')).toBeTruthy();
    fs.rmSync('./deleteme.xxx');

    // Delete the file contents out of DBOS (No different than above)
    // Delete the file contents out of DBOS (using the table index)
    const dfhandle = await testRuntime.invoke(S3Ops).deleteFile(userid, 'text', myFileRecord.file_name, bucket);
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
