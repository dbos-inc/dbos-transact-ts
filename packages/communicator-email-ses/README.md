# DBOS AWS Simple Email Service (SES) Communicator

This is a [DBOS](https://docs.dbos.dev/) [communicator](https://docs.dbos.dev/tutorials/communicator-tutorial) for sending email using the [Amazon Web Services Simple Email Service](https://aws.amazon.com/ses/).

## Getting Started
In order to send emails with SES, it is necessary to:
- Register with AWS and create access keys for SES. (See [Setting up SES](https://docs.aws.amazon.com/ses/latest/dg/setting-up.html) in AWS documentation.)
- Verify a sending domain and destination addresses. (SES will initially be in "sandbox mode", which constrains email sending to be from a validated domain, to a validated email address.  See [Verified identities](https://docs.aws.amazon.com/ses/latest/dg/setting-up.html) in AWS documentation.)

## Configuring a DBOS Application with AWS SES
First, ensure that the DBOS SES communicator is installed into the application:
```
npm install --save @dbos-inc/communicator-email-ses
```

Second, ensure that the communicator is imported into the relevant source file(s):
```typescript
import { SendEmailCommunicator } from "@dbos-inc/communicator-email-ses";
```

Third, place appropriate configuration into the [`dbos-config.yaml`](https://docs.dbos.dev/api-reference/configuration) file; the following example will pull the AWS information from the environment:
```yaml
application:
  aws_ses_configuration: aws_config # Optional if the section is called `aws_config`
  aws_config:
    aws_region: ${AWS_REGION}
    aws_access_key_id: ${AWS_ACCESS_KEY_ID}
    aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

If a different configuration file section should be used for SES, the `aws_ses_configuration` can be changed to indicate a configuration section for use with SES.  If multiple configurations are to be used, the application code will have to name and configure them.

For more information about configuring AWS services, see [AWS Configuration](https://docs.dbos.dev/api-reference/communicatorlib#aws-configuration).

## Selecting A Configuration
`SendEmailCommunicator` is a configured class.  This means that the configuration (or config file key name) must be provided when a class instance is created.  One instance per configuration should be created with `configureInstance` when the application code starts.  For example:
```typescript
import { configureInstance } from "@dbos-inc/dbos-sdk";

// This will use the dbos-config.yaml section named by `aws_ses_configuration` if it is specified, or `aws_config` if not
const defaultSES = configureInstance(SendEmailCommunicator, 'default');
// This will use the section named `aws_config_marketing`
const marketingSES = configureInstance(SendEmailCommunicator, 'marketing', {awscfgname: 'aws_config_marketing'});
```

## Sending Messages
Within a [DBOS Transact Worflow](https://docs.dbos.dev/tutorials/workflow-tutorial), invoke the `SendEmailCommunicator` function from the workflow context:
```typescript
    const result = await workflowContext.invoke(defaultSES).sendEmail(
        {
            to: [workflowContext.getConfig('ses_to_address', 'dbos@nowhere.dev')],
            from: workflowContext.getConfig('ses_from_address', 'info@dbos.dev'),
            subject: 'Test email from DBOS',
            bodyText: 'Check mailbox to see if it worked.'
        }
    );
```

## Sending Templated Messages
Sending a templated email is slightly more involved, as a template must be set up first.  Setting up a template can be invoked as a communicator, or directly (so that it can be called from initialization, or other contexts where a workflow may not be in progress).
- Use `workflowContext.invoke(defaultSES).createEmailTemplate(...)` or `SendEmailCommunicator.createEmailTemplateFunction(...) to create the template.
```typescript
    await workflowContext.invokeOnConfg(defaultSES).createEmailTemplate(
        "testTemplate", {subject: "Email using test template", bodyText: "Today's date is {{todaydate}}."}
    );
```
- Within a workflow, send email with the template, noting that the template substitution data is to be stringified JSON:
```typescript
    await workflowContext.invoke(defaultSES).sendTemplatedEmail({
        to: [workflowContext.getConfig('ses_to_address', 'dbos@nowhere.dev')],
        from: workflowContext.getConfig('ses_from_address', 'info@dbos.dev'),
        templateName: "testTemplate",
        templateDataJSON: JSON.stringify({todaydate: new Date().toISOString()}),
    });
```

## Simple Testing
The `ses.test.ts` file included in the source repository can be used to send an email and a templated email.  Before running, set the following environment variables:
- `SES_FROM_ADDRESS`: An email address within a verified SES sending domain
- `SES_TO_ADDRESS`: Destination email address (which must first be verified with SES if in sandbox mode)
- `AWS_REGION`: AWS region to use
- `AWS_ACCESS_KEY_ID`: The access key with permission to use the SES service
- `AWS_SECRET_ACCESS_KEY`: The secret access key corresponding to `AWS_ACCESS_KEY_ID`

## Notes
While some email services allow setting of a [`Message-ID`](https://en.wikipedia.org/wiki/Message-ID), which would form the foundation of an idempotent email send, SES does not.  This communicator may send duplicate emails in the case of a poorly-timed network or server failure.

## Next Steps
- For a detailed DBOS Transact tutorial, check out our [programming quickstart](https://docs.dbos.dev/getting-started/quickstart-programming).
- To learn how to deploy your application to DBOS Cloud, visit our [cloud quickstart](https://docs.dbos.dev/getting-started/quickstart-cloud/)
- To learn more about DBOS, take a look at [our documentation](https://docs.dbos.dev/) or our [source code](https://github.com/dbos-inc/dbos-transact).
