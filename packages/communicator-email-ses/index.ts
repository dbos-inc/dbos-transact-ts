import {Communicator, CommunicatorContext} from '@dbos-inc/dbos-sdk';

import { SES } from '@aws-sdk/client-ses';

class SendEmailCommunicator
{
    @Communicator()
    static getCurrentTime(_ctx: CommunicatorContext) : Promise<number> {
        return Promise.resolve(new Date().getTime());
    }

    /*
    export async function sendTemplatedEmail(
    ctxt: DBOSContext,
    recipient: string,
    templateData: Record<string, string>
    ): Promise<void> {
    const ses = new SES({
      region: SES_REGION,
      credentials: {
        accessKeyId: ctxt.getConfig('access_key_id') ?? '',
        secretAccessKey: ctxt.getConfig('secret_access_key') ?? '',
      },
    });
    const sourceEmail = ctxt.getConfig('source_email') as string;
    const params = {
        Destination: {
            ToAddresses: [recipient], // Must be verified in SES
        },
        Source: sourceEmail, // Must be verified in SES
        Template: VERIFICATION_LINK_TEMPLATE,
        TemplateData: JSON.stringify(templateData),
    };

    const command = new SendTemplatedEmailCommand(params);
    const _result = await ses.send(command);
    }*/

    // sendEmail, sendTemplatedEmail, sendBulkTemplatedEmail, sendRawEmail, sendCustomVerificationEmail

    // @Communicator() This maybe should not be a communicator
    static async createEmailTemplate(templateName: string, subject: string, text: string) {
        const ses = new SES({
            // TODO
            region: "us-east-1",
            credentials: {
              accessKeyId: "",
              secretAccessKey: "",
            },
        });

        /*
    apiVersion: '2010-12-01',
    region: 'us-east-1',
    credentials: new AWS.Credentials('ACCESS_KEY_ID', 'SECRET_ACCESS_KEY'),
    httpOptions: {
        timeout: 300000,  // 5 minutes
        connectTimeout: 10000  // 10 seconds
    },
    maxRetries: 3,
    logger: console  // Logs API requests and responses to console         */
    
        // Define the email template
        const params = {
            Template: {
                TemplateName: templateName,
                SubjectPart: subject,
                TextPart: text,
            },
        };
        
        // Create the email template
        await ses.createTemplate(params);
        await ses.sendEmail({
            Source:"",
            Destination: {ToAddresses: [""], CcAddresses: [], BccAddresses: []},
            Message: {Subject: {Data: ""}, Body: {Html: {Data: ""}, Text: {Data: "", Charset: 'utf-8'}}}
        });
    }
}

export
{
    SendEmailCommunicator,
}
