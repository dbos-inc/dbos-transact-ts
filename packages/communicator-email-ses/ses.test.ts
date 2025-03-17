import { DBOS_SES } from './index';
export { DBOS_SES };
import { DBOS, parseConfigFile } from '@dbos-inc/dbos-sdk';

describe('ses-tests', () => {
  let sesIsAvailable = true;
  let sesCfg: DBOS_SES | undefined = undefined;

  beforeAll(() => {
    // Check if SES is available and update app config, skip the test if it's not
    if (!process.env['AWS_REGION'] || !process.env['SES_FROM_ADDRESS'] || !process.env['SES_TO_ADDRESS']) {
      sesIsAvailable = false;
    } else {
      // This would normally be a global or static or something
      const [config, rtConfig] = parseConfigFile({ configfile: 'ses-test-dbos-config.yaml' });
      DBOS.setConfig(config, rtConfig);
      sesCfg = new DBOS_SES('default', { awscfgname: 'aws_config' });
    }
  });

  beforeEach(async () => {
    if (sesIsAvailable) {
      await DBOS.launch();
    } else {
      console.log(
        'SES Test is not configured.  To run, set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SES_FROM_ADDRESS, and SES_TO_ADDRESS',
      );
    }
  });

  afterEach(async () => {
    if (sesIsAvailable) {
      await DBOS.shutdown();
    }
  }, 20000);

  test('ses-send', async () => {
    if (!sesIsAvailable || !sesCfg) {
      console.log('SES unavailable, skipping SES tests');
      return;
    }
    const ser = await sesCfg.sendEmail({
      to: [DBOS.getConfig('ses_to_address', 'dbos@nowhere.dev')],
      from: DBOS.getConfig('ses_from_address', 'info@dbos.dev'),
      subject: 'Test email from DBOS SES Unit Test',
      bodyText: 'Check mailbox to see if it worked.',
    });
    expect(ser.MessageId).toBeDefined();

    await sesCfg.createEmailTemplate('unitTestTemplate', {
      subject: 'Email from unit test template',
      bodyText: "Today's date is {{todaydate}}.",
    });
    const ser2 = await sesCfg.sendTemplatedEmail({
      to: [DBOS.getConfig('ses_to_address', 'dbos@nowhere.dev')],
      from: DBOS.getConfig('ses_from_address', 'info@dbos.dev'),
      templateName: 'unitTestTemplate',
      templateDataJSON: JSON.stringify({ todaydate: new Date().toISOString() }),
    });
    expect(ser2.MessageId).toBeDefined();
  });
});
