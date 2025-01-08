import {Communicator, CommunicatorContext, InitContext, ConfiguredInstance, DBOS} from '@dbos-inc/dbos-sdk';

import { SESv2, SendEmailCommand } from '@aws-sdk/client-sesv2';
import { AWSServiceConfig, getAWSConfigForService, loadAWSConfigByName } from '@dbos-inc/aws-config';

export interface SESConfig {
    awscfgname?: string;
    awscfg?: AWSServiceConfig;
}

class DBOS_SES extends ConfiguredInstance
{
    awscfgname?: string = undefined;
    awscfg?: AWSServiceConfig = undefined;

    constructor(name: string, cfg: SESConfig) {
        super(name);
        this.awscfg = cfg.awscfg;
        this.awscfgname = cfg.awscfgname;
    }

    static AWS_SES_CONFIGURATION = 'aws_ses_configuration';
    async initialize(ctx: InitContext) {
        // Get the config and call the validation
        if (!this.awscfg) {
            if (this.awscfgname) {
                this.awscfg = loadAWSConfigByName(ctx, this.awscfgname);
            }
            else {
                this.awscfg = getAWSConfigForService(ctx, DBOS_SES.AWS_SES_CONFIGURATION);
            }
        }
        if (!this.awscfg) {
            throw new Error(`AWS Configuration not specified for DBOS_SES: ${this.name}`);
        }
        return Promise.resolve();
    }

    @DBOS.step()
    async sendEmail(
        mail: {to?:string[], cc?:string[], bcc?:string[], from:string, subject: string, bodyHtml?:string, bodyText?:string}
    )
    {
        try {
            const cfg = this.awscfg!;
            const ses = DBOS_SES.createSES(cfg);

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
            DBOS.logger.error(e);
            throw e;
        }
    }

    @DBOS.step()
    async sendTemplatedEmail(
        templatedMail: {to?: string[], cc?: string[], bcc?: string[], from: string,
            templateName: string,
            templateDataJSON:string /*Record<string, string>*/
        }
    )
    {
        const cfg = this.awscfg!;
        const ses = DBOS_SES.createSES(cfg);
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
            endpoint: cfg.endpoint,
            region: cfg.region,
            credentials: cfg.credentials,
            maxAttempts: cfg.maxRetries,
            //logger: console,
        });
    }

    /*
     * Create SES Email Template - This is the non-DBOS version
     */
    static async createEmailTemplateFunction(
        cfg: AWSServiceConfig,
        templateName: string,
        template: {subject: string, bodyHtml?:string, bodyText?:string}
    ) {
        // Create SES
        const ses = DBOS_SES.createSES(cfg);
    
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
    @DBOS.step()
    async createEmailTemplate(
        templateName:string,
        template: {subject: string, bodyHtml?:string, bodyText?:string}
    ) {
        return await DBOS_SES.createEmailTemplateFunction(this.awscfg!, templateName, template);
    }
}

// Older interface
class SendEmailCommunicator extends ConfiguredInstance
{
    awscfgname?: string = undefined;
    awscfg?: AWSServiceConfig = undefined;

    constructor(name: string, cfg: SESConfig) {
        super(name);
        this.awscfg = cfg.awscfg;
        this.awscfgname = cfg.awscfgname;
    }

    static AWS_SES_CONFIGURATION = 'aws_ses_configuration';
    async initialize(ctx: InitContext) {
        // Get the config and call the validation
        if (!this.awscfg) {
            if (this.awscfgname) {
                this.awscfg = loadAWSConfigByName(ctx, this.awscfgname);
            }
            else {
                this.awscfg = getAWSConfigForService(ctx, SendEmailCommunicator.AWS_SES_CONFIGURATION);
            }
        }
        if (!this.awscfg) {
            throw new Error(`AWS Configuration not specified for SendEmailCommunicator: ${this.name}`);
        }
        return Promise.resolve();
    }

    static createSES(cfg: AWSServiceConfig) {
        return new SESv2({
            endpoint: cfg.endpoint,
            region: cfg.region,
            credentials: cfg.credentials,
            maxAttempts: cfg.maxRetries,
            //logger: console,
        });
    }

    @Communicator()
    async sendEmail(
        ctx: CommunicatorContext,
        mail: {to?:string[], cc?:string[], bcc?:string[], from:string, subject: string, bodyHtml?:string, bodyText?:string}
    )
    {
        try {
            const cfg = this.awscfg!;
            const ses = DBOS_SES.createSES(cfg);

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
    async sendTemplatedEmail(
        _ctx: CommunicatorContext,
        templatedMail: {to?: string[], cc?: string[], bcc?: string[], from: string,
            templateName: string,
            templateDataJSON:string /*Record<string, string>*/
        }
    )
    {
        const cfg = this.awscfg!;
        const ses = DBOS_SES.createSES(cfg);
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

    /*
     * Create SES Email Template - This is the non-communicator version
     */
    static async createEmailTemplateFunction(
        cfg: AWSServiceConfig,
        templateName: string,
        template: {subject: string, bodyHtml?:string, bodyText?:string}
    ) {
        return DBOS_SES.createEmailTemplateFunction(cfg, templateName, template);
    }

    /*
     * Create SES Email template - Communicator version
     */
    @Communicator()
    async createEmailTemplate(
        _ctx:CommunicatorContext,
        templateName:string,
        template: {subject: string, bodyHtml?:string, bodyText?:string}
    ) {
        return await DBOS_SES.createEmailTemplateFunction(this.awscfg!, templateName, template);
    }
}


export {
    SendEmailCommunicator,
    SendEmailCommunicator as SendEmailStep,
    DBOS_SES,
}