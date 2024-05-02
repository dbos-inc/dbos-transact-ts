import {Communicator, CommunicatorContext, DBOSInitializer, InitContext} from '@dbos-inc/dbos-sdk';

import { SES } from '@aws-sdk/client-ses';
import { AWSServiceConfig, getAWSConfigForService, getAWSConfigs } from './awscfg';

export class SendEmailCommunicator
{
    static AWS_SES_CONFIGURATIONS = 'aws_ses_configurations';
    static AWS_SES_API_VERSION = '2010-12-01';

    @DBOSInitializer()
    static checkConfig(ctx: InitContext) : Promise<void> {
        // Get the config and call the validation
        getAWSConfigs(ctx, SendEmailCommunicator.AWS_SES_CONFIGURATIONS);
        return Promise.resolve();
    }

    @Communicator()
    static async sendEmail(
        _ctx: CommunicatorContext,
        _cfg:{stage: string, awscfg: string},
        _mail:{to?:string[], cc?:string[], bcc?:string, from:string, body:string})
    {
        /*
        await ses.sendEmail({
        Source:"",
        Destination: {ToAddresses: [""], CcAddresses: [], BccAddresses: []},
        Message: {Subject: {Data: ""}, Body: {Html: {Data: ""}, Text: {Data: "", Charset: 'utf-8'}}}
        });
        */
    }

    @Communicator()
    static async sendTemplatedEmail(
        _ctx: CommunicatorContext,
        _cfg:{stage: string, awscfg: string},
        _template:{recipients:string[], from:string, template:string, templateDataJSON:string}
    )
    {
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
        */
    }


    // Could also consider ...
    // sendBulkTemplatedEmail, sendRawEmail, sendCustomVerificationEmail

    static createSES(cfg: AWSServiceConfig) {
        return new SES({
            region: cfg.region,
            credentials: cfg.credentials,
            maxAttempts: cfg.maxRetries,
            //apiVersion: SendEmailCommunicator.AWS_SES_API_VERSION,
            //logger: console,
        });
    }

    /*
     * Create SES Email Template - This is the non-communicator version
     */
    static async createEmailTemplateFunction(cfg: AWSServiceConfig, templateName: string, subject: string, text: string) {
        // Create SES
        const ses = SendEmailCommunicator.createSES(cfg);
    
        // Define the email template
        const params = {
            Template: {
                TemplateName: templateName,
                SubjectPart: subject,
                TextPart: text,
            },
        };
        
        // Send call to create the email template
        await ses.createTemplate(params);
    }

    /*
     * Create SES Email template - Communicator version
     */
    @Communicator()
    static async createEmailTemplate(ctx:CommunicatorContext, cfgname:string, templateName: string, subject: string, text: string) {
        const cfg = getAWSConfigForService(ctx, SendEmailCommunicator.AWS_SES_CONFIGURATIONS, cfgname);
        return await this.createEmailTemplateFunction(cfg, templateName, subject, text);
    }
}
