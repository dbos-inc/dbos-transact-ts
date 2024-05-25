import {Communicator, CommunicatorContext, Configurable, InitContext} from '@dbos-inc/dbos-sdk';

import { SESv2, SendEmailCommand } from '@aws-sdk/client-sesv2';
import { AWSServiceConfig, getAWSConfigForService, getAWSConfigs } from '@dbos-inc/aws-config';

interface SESConfig{
    awscfgname?: string,
    awscfg?: AWSServiceConfig,
}

@Configurable()
class SendEmailCommunicator
{
    static AWS_SES_CONFIGURATIONS = 'aws_ses_configurations';
    static async initConfiguration(ctx: InitContext, arg: SESConfig) {
        // Get the config and call the validation
        getAWSConfigs(ctx, SendEmailCommunicator.AWS_SES_CONFIGURATIONS);
        if (!arg.awscfg) {
            arg.awscfg = getAWSConfigForService(ctx, this.AWS_SES_CONFIGURATIONS, arg.awscfgname ?? "");
        }
        return Promise.resolve();
    }

    @Communicator()
    static async sendEmail(
        ctx: CommunicatorContext,
        mail: {to?:string[], cc?:string[], bcc?:string[], from:string, subject: string, bodyHtml?:string, bodyText?:string}
    )
    {
        try {
            const cfg = ctx.getClassConfig<SESConfig>().awscfg!;
            const ses = SendEmailCommunicator.createSES(cfg);

            return await ses.sendEmail({
                FromEmailAddress: mail.from,
                Destination: {ToAddresses: mail.to, CcAddresses: mail.cc, BccAddresses: mail.bcc},
                Content: {
                    Simple: {
                        Subject: {Data: mail.subject},
                        Body: {Html: (mail.bodyHtml ? {Data: mail.bodyHtml} : undefined),
                            Text: (mail.bodyText ? {Data: mail.bodyText, Charset: 'utf-8'}: undefined)}
                    }
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
        }
    )
    {
        const cfg = ctx.getClassConfig<SESConfig>().awscfg!;
        const ses = SendEmailCommunicator.createSES(cfg);
        const command = new SendEmailCommand(
            {
                Destination: {
                    ToAddresses: templatedMail.to,
                    CcAddresses: templatedMail.cc,
                    BccAddresses: templatedMail.bcc,
                },
                FromEmailAddress: templatedMail.from, // Must be verified in SES
                Content: {
                    Template: {
                        TemplateName: templatedMail.templateName,
                        TemplateData: templatedMail.templateDataJSON,
                    }
                }
            }
        );
        return await ses.send(command);
    }

    // Could also consider ...
    // sendBulkTemplatedEmail, sendRawEmail, sendCustomVerificationEmail

    static createSES(cfg: AWSServiceConfig) {
        return new SESv2({
            region: cfg.region,
            credentials: cfg.credentials,
            maxAttempts: cfg.maxRetries,
            //logger: console,
        });
    }

    /*
     * Create SES Email Template - This is the non-communicator version
     */
    static async createEmailTemplateFunction(
        cfg: AWSServiceConfig,
        templateName: string,
        template: {subject: string, bodyHtml?:string, bodyText?:string}
    ) {
        // Create SES
        const ses = SendEmailCommunicator.createSES(cfg);
    
        // Define the email template
        const params = {
            TemplateName: templateName,
            TemplateContent: {
                Subject: template.subject,
                Text: template.bodyText,
                Html: template.bodyHtml,
            }
        };

        // Check for existing template
        let existingTemplate = false;
        try {
            const exists = await ses.getEmailTemplate({TemplateName: templateName});
            if (exists.TemplateContent?.Subject) {
                existingTemplate = true;
            }
        }
        catch (e) {
            // Doesn't exist, or the next error is more important
        }

        // Send call to create the email template
        if (!existingTemplate) {
            await ses.createEmailTemplate(params);
        }
        else {
            await ses.updateEmailTemplate(params);
        }
    }

    /*
     * Create SES Email template - Communicator version
     */
    @Communicator()
    static async createEmailTemplate(
        ctx:CommunicatorContext,
        templateName:string,
        template: {subject: string, bodyHtml?:string, bodyText?:string}
    ) {
        const cfg = ctx.getClassConfig<SESConfig>().awscfg!;
        return await SendEmailCommunicator.createEmailTemplateFunction(cfg, templateName, template);
    }
}

export {
    SendEmailCommunicator
}
