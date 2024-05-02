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
 *  aws_timeout: (optional, is defaulted in the code for the communicator)
 *  aws_connect_timeout: (optional, is defaulted in the code for the communicator)
 *  aws_max_retries: (optional, is defaulted in the code for the communicator)
 *
 * Then, each communicator module will support a list of AWS configurations, for
 *  example:
 * aws_ses_configurations: my_aws_config
 * 
 * By providing this list, the communicator can validate configuration information
 *  at app startup.
 * By default, these will just use a configuration called 'aws_config'.
 * 
 * When invoking the component, if there is more than one choice of config, it can
 *   be specified to the call.  The default will be the first in the list.
 */

import { DBOSConfigKeyTypeError, DBOSError } from "../../src/error";

interface ConfigProvider
{
    getConfig<T>(key: string): T | undefined;
    getConfig<T>(key: string, defaultValue: T): T;
}

export interface AWSServiceConfig
{
    name: string,
    region: string,
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
    aws_access_key_id?: string,
    aws_secret_access_key?: string,
    aws_timeout?: number,
    aws_connect_timeout?: number,
    aws_max_retries?: number,
}

export function loadAWSConfigByName(ctx: ConfigProvider, cfgname: string): AWSServiceConfig {
    const cfgstrs = ctx.getConfig<AWSCfgFileItem|undefined>(cfgname, undefined);
    if (!cfgstrs) {
        throw new DBOSConfigKeyTypeError(cfgname, 'AWSCfgFileItem', 'null');
    }

    if (!cfgstrs.aws_region || typeof(cfgstrs.aws_region) !== 'string') {
        throw new DBOSConfigKeyTypeError(`${cfgname}.aws_region`, 'string', typeof(cfgstrs.aws_region));
    }
    if (!cfgstrs.aws_access_key_id || typeof(cfgstrs.aws_access_key_id) !== 'string') {
        throw new DBOSConfigKeyTypeError(`${cfgname}.aws_access_key_id`, 'string', typeof(cfgstrs.aws_access_key_id));
    }
    if (!cfgstrs.aws_secret_access_key || typeof(cfgstrs.aws_secret_access_key) !== 'string') {
        throw new DBOSConfigKeyTypeError(`${cfgname}.aws_secret_access_key`, 'string', typeof(cfgstrs.aws_secret_access_key));
    }

    return {name: cfgname, region: '', credentials: {accessKeyId: "", secretAccessKey:""}};

}

export function loadAWSCongfigsByNames(ctx: ConfigProvider, cfgnames: string) {
    const configs: AWSServiceConfig[] = [];
    const cfgnamesarr = (cfgnames || 'aws_config').split(',');

    for (const cfgname of cfgnamesarr) {
        configs.push(loadAWSConfigByName(ctx, cfgname))
    }

    return configs;
}

export function getAWSConfigs(ctx: ConfigProvider, svccfgname?: string) : AWSServiceConfig[]
{
    if (svccfgname) {
        return loadAWSCongfigsByNames(ctx, ctx.getConfig<string>(svccfgname, ''));
    }
    return [loadAWSConfigByName(ctx, 'aws_config')];
}

export function getAWSConfigForService(ctx: ConfigProvider, svccfgname: string, cfgname: string) : AWSServiceConfig
{
    if (svccfgname) {
        const cfgs = loadAWSCongfigsByNames(ctx, ctx.getConfig<string>(svccfgname, ''));
        for (const cfg of cfgs) {
            if (cfg.name.toLowerCase() === cfgname || !cfgname) return cfg;
        }
        throw new DBOSError(`Configuration '${cfgname}' does not exist in service '${svccfgname}'`);
    }
    return loadAWSConfigByName(ctx, cfgname || 'aws_config');
}
