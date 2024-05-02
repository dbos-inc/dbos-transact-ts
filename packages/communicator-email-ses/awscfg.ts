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

import { DBOSContext } from "@dbos-inc/dbos-sdk";

export interface AWSServiceConfig
{
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

export function getAWSConfigs(_svccfgname: string, _ctx: DBOSContext) : AWSServiceConfig[]
{
    const configs: AWSServiceConfig[] = [];
    return configs;
}