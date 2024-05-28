# DBOS Config File Utilities for AWS

Common utility for configuring AWS services in a DBOS Transact application.
 
The approach is to decompose AWS credentials into two parts, to allow sharing
of the credential between services, or separate credentials per service.
 
Start by giving a name to a configuration for an AWS service or role.
In dbos-config.yaml, under 'application', make a section with all the
AWS bits:
```yaml
 my_aws_config:
  aws_region: us-east-2
  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
```

Then, each communicator module will support a list of AWS configurations, for
example SES (Simple Email Service) uses:
```yaml
 aws_ses_configurations: my_aws_config
```
 
By providing this list, the communicator can validate configuration information
at app startup.

By default, these will just use a configuration called 'aws_config'.

## Interfaces

The subsection of `application` in the configuration file should provide:
```typescript
export interface AWSCfgFileItem
{
    aws_region?: string,
    aws_access_key_id?: string,
    aws_secret_access_key?: string,
}
```

The interpretation of the configuration file entry will be provided in the following interface:
```typescript
export interface AWSServiceConfig
{
    name: string,
    region: string,
    credentials: {
      accessKeyId: string,
      secretAccessKey: string,
    },
}
```

## Available Functions
```typescript
// Loads an AWS configuration by its section name within the `application` part of dbos-config.yaml
export function loadAWSConfigByName(ctx: ConfigProvider, cfgname: string): AWSServiceConfig

// Loads multiple AWS configurations by section name within the `application` part of dbos-config.yaml
export function loadAWSCongfigsByNames(ctx: ConfigProvider, cfgnames: string): AWSServiceConfig[]

// Reads a key from within dbos-config.yaml and uses the value of that key to load one or more AWS configuration sections
export function getAWSConfigs(ctx: ConfigProvider, svccfgname?: string) : AWSServiceConfig[]

// Reads a key `svccfgname` from within dbos-config.yaml and uses the value of that key to load one or more AWS configuration sections.  If there is more than one, then the one named `cfgname` is loaded.
//  If `svccfgname` is not provided or doesn't exist, then the configuration section indicated by `cfgname` is loaded.
//  If `cfgname` is not provided, then `the aws_config` section is loaded
export function getAWSConfigForService(ctx: ConfigProvider, svccfgname: string, cfgname: string) : AWSServiceConfig
```
