import {ArgOptional, Communicator, CommunicatorContext, DBOSInitializer, InitContext} from '@dbos-inc/dbos-sdk';

import { SES, SendTemplatedEmailCommand } from '@aws-sdk/client-ses';
import { AWSServiceConfig, getAWSConfigForService, getAWSConfigs } from './awscfg';

class SendEmailCommunicator
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
        ctx: CommunicatorContext,
        mail: {to?:string[], cc?:string[], bcc?:string[], from:string, subject: string, bodyHtml?:string, bodyText?:string},
        @ArgOptional
        config?: {
            configName?: string,
            workflowStage?: string,
        }
    )
    {
        try {
            const cfg = getAWSConfigForService(ctx, SendEmailCommunicator.AWS_SES_CONFIGURATIONS, config?.configName ?? "");
            const ses = SendEmailCommunicator.createSES(cfg);

            return await ses.sendEmail({
                Source: mail.from,
                Destination: {ToAddresses: mail.to, CcAddresses: mail.cc, BccAddresses: mail.bcc},
                Message: {
                    Subject: {Data: mail.subject},
                    Body: {Html: (mail.bodyHtml ? {Data: mail.bodyHtml} : undefined),
                        Text: (mail.bodyText ? {Data: mail.bodyText, Charset: 'utf-8'}: undefined)}
                }
            });
        }
        catch (e) {
            ctx.logger.error(e);
            throw e;
        }
    }

    @Communicator()
    static async sendTemplatedEmail(
        ctx: CommunicatorContext,
        templatedMail: {to?: string[], cc?: string[], bcc?: string[], from: string,
            templateName: string,
            templateDataJSON:string /*Record<string, string>*/
        },
        @ArgOptional
        config?: {
            configName?: string,
            workflowStage?: string,
        }
    )
    {
        const cfg = getAWSConfigForService(ctx, SendEmailCommunicator.AWS_SES_CONFIGURATIONS, config?.configName ?? "");
        const ses = SendEmailCommunicator.createSES(cfg);
        const command = new SendTemplatedEmailCommand(
            {
                Destination: {
                    ToAddresses: templatedMail.to,
                    CcAddresses: templatedMail.cc,
                    BccAddresses: templatedMail.bcc,
                },
                Source: templatedMail.from, // Must be verified in SES
                Template: templatedMail.templateName,
                TemplateData: templatedMail.templateDataJSON,
            }
        );
        return await ses.send(command);
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
        return await SendEmailCommunicator.createEmailTemplateFunction(cfg, templateName, subject, text);
    }
}

export {
    SendEmailCommunicator
}