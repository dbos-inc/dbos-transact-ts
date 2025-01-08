/**
 * Common utility for AWS configuration in application.
 * 
 * The approach is to decompose AWS credentials into two parts, to allow sharing
 *  of the credential between services, or separate credentials per service.
 * 
 * Start by giving a name to a configuration for an AWS service or role.
 *  In dbos-config.yaml, under 'application', make a section with all the
 *  AWS bits:
 * my_aws_config:
 *  aws_region: us-east-2
 *  aws_access_key_id: ${AWS_ACCESS_KEY_ID}
 *  aws_secret_access_key: ${AWS_SECRET_ACCESS_KEY}
 *
 * Then, each communicator module will support a named AWS configuration, for
 *  example:
 * aws_ses_configuration: my_aws_config
 * 
 * By providing this list, the communicator can validate configuration information
 *  at app startup.
 * By default, these will just use a configuration called 'aws_config'.
 * 
 * When invoking the component, if there is more than one choice of config, it can
 *   will be configured into the instance.
 */

import { Error as DBOSError, DBOS } from "@dbos-inc/dbos-sdk";

interface ConfigProvider
{
    getConfig<T>(key: string): T | undefined;
    getConfig<T>(key: string, defaultValue: T): T;
}

class DBOSCP implements ConfigProvider
{
    getConfig<T>(key: string): T | undefined;
    getConfig<T>(key: string, defaultValue: T): T;
    getConfig<T>(key: string, defaultValue?: T) {return DBOS.getConfig(key, defaultValue)};
}

export interface AWSServiceConfig
{
    name: string,
    region: string,
    endpoint?: string,
    credentials: {
      accessKeyId: string,
      secretAccessKey: string,
    },
    httpOptions?: {
        timeout?: number,
        connectTimeout?: number,
    },
    maxRetries?: number
}

export interface AWSCfgFileItem
{
    aws_region?: string,
    aws_endpoint?: string,
    aws_access_key_id?: string,
    aws_secret_access_key?: string,
}

// Load a config by its section name
export function loadAWSConfigByName(ctx: ConfigProvider, cfgname: string): AWSServiceConfig {
    const cfgstrs = ctx.getConfig<AWSCfgFileItem|undefined>(cfgname, undefined);
    if (!cfgstrs) {
        throw new DBOSError.DBOSConfigKeyTypeError(cfgname, 'AWSCfgFileItem', 'null');
    }

    if (!cfgstrs.aws_region) {
        throw new DBOSError.DBOSError(`aws_region not specified in configuration ${cfgname}`);
    }
    if (!cfgstrs.aws_region || typeof(cfgstrs.aws_region) !== 'string') {
        throw new DBOSError.DBOSConfigKeyTypeError(`${cfgname}.aws_region`, 'string', typeof(cfgstrs.aws_region));
    }

    if (!cfgstrs.aws_access_key_id) {
        throw new DBOSError.DBOSError(`aws_access_key_id not specified in configuration ${cfgname}`);
    }
    if (typeof(cfgstrs.aws_access_key_id) !== 'string') {
        throw new DBOSError.DBOSConfigKeyTypeError(`${cfgname}.aws_access_key_id`, 'string', typeof(cfgstrs.aws_access_key_id));
    }

    if (!cfgstrs.aws_secret_access_key) {
        throw new DBOSError.DBOSError(`aws_secret_access_key not specified in configuration ${cfgname}`);
    }
    if (typeof(cfgstrs.aws_secret_access_key) !== 'string') {
        throw new DBOSError.DBOSConfigKeyTypeError(`${cfgname}.aws_secret_access_key`, 'string', typeof(cfgstrs.aws_secret_access_key));
    }

    if (cfgstrs.aws_endpoint && typeof(cfgstrs.aws_region) !== 'string') {
        throw new DBOSError.DBOSConfigKeyTypeError(`${cfgname}.aws_endpoint`, 'string', typeof(cfgstrs.aws_endpoint));
    }

    return {
        name: cfgname,
        region: cfgstrs.aws_region.toString(),
        endpoint: cfgstrs.aws_endpoint ? cfgstrs.aws_endpoint : undefined,
        credentials: {
            accessKeyId: cfgstrs.aws_access_key_id.toString(),
            secretAccessKey: cfgstrs.aws_secret_access_key.toString()
        }
    };
}

export function getAWSConfigByName(cfgname: string): AWSServiceConfig {
    return loadAWSConfigByName(new DBOSCP(), cfgname);
}

export function getAWSConfigForService(ctx: ConfigProvider, svccfgname: string) : AWSServiceConfig
{
    if (svccfgname && ctx.getConfig<string>(svccfgname, '')) {
        return loadAWSConfigByName(ctx, ctx.getConfig<string>(svccfgname, ''));
    }
    return loadAWSConfigByName(ctx, 'aws_config');
}

export function getConfigForAWSService(svccfgname: string) {
    return getAWSConfigForService(new DBOSCP(), svccfgname);
}