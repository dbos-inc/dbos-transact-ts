# DBOS Config File Utilities for AWS

Common utility for configuring AWS services in a DBOS application.

By default, services just use a configuration called 'aws_config'.

Start by giving a name to a configuration for an AWS service or role.
In `dbos-config.yaml`, under 'application', make a section with all the
AWS bits:

```yaml
aws_config:
  aws_region: us-east-2
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

It is also possible to have more than one configuration, so that each service
has its own configuration. In this case, additional AWS configuration sets are
placed in `dbos-config.yaml`:

```yaml
my_aws_config_for_ses:
  aws_region: us-east-2
  aws_access_key_id: ${AWS_ACCESS_KEY_ID_SES}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY_SES}
```

Then, each communicator library supports a default AWS configuration name, for
example SES (Simple Email Service) uses:

```yaml
aws_ses_configuration: my_aws_config_for_ses
```

If the application uses more than one set of credentials for the same service,
such as a separate email configuration for sending advertising vs order confirmations,
then separate sections will be specified within the application code.

```yaml
ses_config_for_marketing:
  aws_region: ${AWS_REGION}
  aws_access_key_id: ${AWS_ACCESS_KEY_ID_SES1}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY_SES1}
ses_config_for_orders:
  aws_region: ${AWS_REGION}
  aws_access_key_id: ${AWS_ACCESS_KEY_ID_SES1}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY_SES1}
```

Most AWS services support endpoints. The endpoint may be included in the configuration as desired:

```yaml
sqs_for_shipping:
  aws_region: ${AWS_REGION}
  aws_endpoint: ${AWS_ENDPOINT_URL_SQS}
  aws_access_key_id: 'notneededintest'
  aws_secret_access_key: 'notneededintest'
```

## Interfaces

The subsection of `application` in the configuration file should provide:

```typescript
export interface AWSCfgFileItem {
  aws_region?: string;
  aws_endpoint?: string;
  aws_access_key_id?: string;
  aws_secret_access_key?: string;
}
```

The interpretation of the configuration file entry will be provided in the following interface:

```typescript
export interface AWSServiceConfig {
  name: string;
  region: string;
  endpoint?: string;
  credentials: {
    accessKeyId: string;
    secretAccessKey: string;
  };
}
```

## Available Functions

```typescript
// Loads an AWS configuration by its section name within the `application` part of dbos-config.yaml
function getAWSConfigByName(cfgname: string): AWSServiceConfig;

// Reads a key from within dbos-config.yaml and uses the value of that key to load an AWS configuration section
function getConfigForAWSService(svccfgname: string): AWSServiceConfig;
```
