import { SendEmailCommunicator } from "./index";
export { SendEmailCommunicator };
import { TestingRuntime, createTestingRuntime, initClassConfiguration } from "@dbos-inc/dbos-sdk";

// This would normally be a global or static or something
const sesCfg = initClassConfiguration(SendEmailCommunicator, 'default', {awscfgname: 'aws_config'});

describe("ses-tests", () => {
  let testRuntime: TestingRuntime | undefined = undefined;
  let sesIsAvailable = true;

  beforeAll(() => {
    // Check if SES is available and update app config, skip the test if it's not
    if (!process.env['AWS_REGION'] || !process.env['SES_FROM_ADDRESS'] || !process.env['SES_TO_ADDRESS']) {
      sesIsAvailable = false;
    }
  });

  beforeEach(async () => {
    if (sesIsAvailable) {
      testRuntime = await createTestingRuntime([SendEmailCommunicator],'ses-test-dbos-config.yaml');
    }
    else {
      console.log("SES Test is not configured.  To run, set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SES_FROM_ADDRESS, and SES_TO_ADDRESS");
    }
  });

  afterEach(async () => {
    if (sesIsAvailable) {
      await testRuntime?.destroy();
    }
  }, 10000);

  test("ses-send", async () => {
    if (!sesIsAvailable || !testRuntime) {
      console.log("SES unavailable, skipping SES tests");
      return;
    }
    const ser = await testRuntime.invokeConfig(SendEmailCommunicator).sendEmail(
        {
            to: [testRuntime.getConfig('ses_to_address', 'dbos@nowhere.dev')],
            from: testRuntime.getConfig('ses_from_address', 'info@dbos.dev'),
            subject: 'Test email from DBOS SES Unit Test',
            bodyText: 'Check mailbox to see if it worked.'
        },
        //{configName: 'aws_config_alt'}
    );
    expect(ser.MessageId).toBeDefined();

    await testRuntime.invokeConfig(SendEmailCommunicator).createEmailTemplate(
        "unitTestTemplate", {subject: "Email from unit test template", bodyText: "Today's date is {{todaydate}}."},
        //{configName: 'aws_config_alt'}
    );
    const ser2 = await testRuntime.invokeConfig(SendEmailCommunicator).sendTemplatedEmail({
        to: [testRuntime.getConfig('ses_to_address', 'dbos@nowhere.dev')],
        from: testRuntime.getConfig('ses_from_address', 'info@dbos.dev'),
        templateName: "unitTestTemplate",
        templateDataJSON: JSON.stringify({todaydate: new Date().toISOString()}),
    },
    //{configName: 'aws_config_alt'}
    );
    expect(ser2.MessageId).toBeDefined();
  });
});
