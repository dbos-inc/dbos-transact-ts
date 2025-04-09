import {
  DBOS,
  Communicator,
  CommunicatorContext,
  InitContext,
  ConfiguredInstance,
  Error as DBOSError,
  DBOSEventReceiver,
  DBOSExecutorContext,
  WorkflowContext,
  WorkflowFunction,
  associateClassWithEventReceiver,
  associateMethodWithEventReceiver,
} from '@dbos-inc/dbos-sdk';

import {
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueAttributesCommandInput,
  Message,
  ReceiveMessageCommand,
  ReceiveMessageCommandOutput,
  SQSClient,
  SendMessageCommand,
  SendMessageCommandInput,
} from '@aws-sdk/client-sqs';
import {
  AWSServiceConfig,
  getAWSConfigByName,
  getAWSConfigForService,
  getConfigForAWSService,
  loadAWSConfigByName,
} from '@dbos-inc/aws-config';

// Create a new type that omits the QueueUrl property
type MessageWithoutQueueUrl = Omit<SendMessageCommandInput, 'QueueUrl'>;

// Create a new type that allows QueueUrl to be added later
type MessageWithOptionalQueueUrl = MessageWithoutQueueUrl & { QueueUrl?: string };

interface SQSConfig {
  awscfgname?: string;
  awscfg?: AWSServiceConfig;
  queueUrl?: string;
  queueURL?: string; // disfavored vs. queueUrl
  getWFKey?: (m: Message) => string;
  workflowQueueName?: string;
}

class DBOS_SQS extends ConfiguredInstance {
  config: SQSConfig;
  client?: SQSClient;

  constructor(name: string, cfg: SQSConfig) {
    super(name);
    this.config = cfg;
  }

  static AWS_SQS_CONFIGURATION = 'aws_sqs_configuration';
  async initialize() {
    DBOS.logger.info(`DBOS_SQS initialize`);

    // Get the config and call the validation
    if (!this.config.awscfg) {
      if (this.config.awscfgname) {
        this.config.awscfg = getAWSConfigByName(this.config.awscfgname);
      } else {
        this.config.awscfg = getConfigForAWSService(DBOS_SQS.AWS_SQS_CONFIGURATION);
      }
    }
    if (!this.config.awscfg) {
      throw new Error(`AWS Configuration not specified for DBOS_SQS: ${this.name}`);
    }
    this.client = DBOS_SQS.createSQS(this.config.awscfg);
    return Promise.resolve();
  }

  static async validateConnection(client: SQSClient, url: string) {
    const params: GetQueueAttributesCommandInput = {
      QueueUrl: url,
      AttributeNames: ['All'],
    };

    const _validateSQSConfiguration = await client.send(new GetQueueAttributesCommand(params));
  }

  @DBOS.step()
  async sendMessage(msg: MessageWithOptionalQueueUrl) {
    try {
      const smsg = { ...msg, QueueUrl: msg.QueueUrl || this.config.queueUrl || this.config.queueURL };
      return await this.client!.send(new SendMessageCommand(smsg));
    } catch (e) {
      DBOS.logger.error(e);
      throw e;
    }
  }

  static createSQS(cfg: AWSServiceConfig) {
    return new SQSClient({
      region: cfg.region,
      credentials: cfg.credentials,
      maxAttempts: cfg.maxRetries,
      endpoint: cfg.endpoint,
      //logger: console,
    });
  }
}

class SQSCommunicator extends ConfiguredInstance {
  config: SQSConfig;
  client?: SQSClient;

  constructor(name: string, cfg: SQSConfig) {
    super(name);
    this.config = cfg;
  }

  async initialize(ctx: InitContext) {
    // Get the config and call the validation
    if (!this.config.awscfg) {
      if (this.config.awscfgname) {
        this.config.awscfg = loadAWSConfigByName(ctx, this.config.awscfgname);
      } else {
        this.config.awscfg = getAWSConfigForService(ctx, DBOS_SQS.AWS_SQS_CONFIGURATION);
      }
    }
    if (!this.config.awscfg) {
      throw new Error(`AWS Configuration not specified for SQSCommunicator: ${this.name}`);
    }
    this.client = DBOS_SQS.createSQS(this.config.awscfg);
    return Promise.resolve();
  }

  static async validateConnection(client: SQSClient, url: string) {
    return DBOS_SQS.validateConnection(client, url);
  }

  static createSQS(cfg: AWSServiceConfig) {
    return new SQSClient({
      region: cfg.region,
      credentials: cfg.credentials,
      maxAttempts: cfg.maxRetries,
      endpoint: cfg.endpoint,
      //logger: console,
    });
  }

  @Communicator()
  async sendMessage(ctx: CommunicatorContext, msg: MessageWithOptionalQueueUrl) {
    try {
      const smsg = { ...msg, QueueUrl: msg.QueueUrl || this.config.queueUrl || this.config.queueURL };
      return await this.client!.send(new SendMessageCommand(smsg));
    } catch (e) {
      ctx.logger.error(e);
      throw e;
    }
  }
}

interface SQSReceiverClassDefaults {
  config?: SQSConfig;
}

interface SQSReceiverMethodSpecifics {
  config?: SQSConfig;
}

class SQSReceiver implements DBOSEventReceiver {
  executor?: DBOSExecutorContext;
  listeners: Promise<void>[] = [];
  isShuttingDown = false;

  async destroy() {
    this.isShuttingDown = true;
    try {
      await Promise.allSettled(this.listeners);
    } catch (e) {
      // yawn
    }
    this.listeners = [];
  }

  // async function that uses .then/.catch to handle potentially unreliable library calls
  static async sendMessageSafe(sqs: SQSClient, params: ReceiveMessageCommand): Promise<ReceiveMessageCommandOutput> {
    return new Promise((resolve, reject) => {
      sqs
        .send(params)
        .then((response) => resolve(response))
        .catch((error) => reject(error as Error));
    });
  }

  async initialize(executor: DBOSExecutorContext) {
    this.executor = executor;
    const regops = this.executor.getRegistrationsFor(this);
    for (const registeredOperation of regops) {
      const cro = registeredOperation.classConfig as SQSReceiverClassDefaults;
      const mro = registeredOperation.methodConfig as SQSReceiverMethodSpecifics;
      const url = cro.config?.queueUrl ?? cro.config?.queueURL ?? mro.config?.queueUrl ?? mro.config?.queueURL;

      if (url) {
        const method = registeredOperation.methodReg;
        const cname = method.className;
        const mname = method.name;
        if (!method.workflowConfig) {
          throw new DBOSError.DBOSError(
            `Error registering method ${cname}.${mname}: An SQS decorator can only be assigned to a workflow!`,
          );
        }
        let awscfg = mro.config?.awscfg;
        if (!awscfg && mro.config?.awscfgname) {
          awscfg = loadAWSConfigByName(this.executor, mro.config.awscfgname);
        }
        if (!awscfg && cro.config?.awscfg) {
          awscfg = cro.config.awscfg;
        }
        if (!awscfg && cro.config?.awscfgname) {
          awscfg = loadAWSConfigByName(this.executor, cro.config.awscfgname);
        }
        if (!awscfg) {
          awscfg = getAWSConfigForService(this.executor, DBOS_SQS.AWS_SQS_CONFIGURATION);
        }
        if (!awscfg) {
          throw new DBOSError.DBOSError(`AWS Configuration not specified for SQS Receiver: ${cname}.${mname}`);
        }

        const sqs = DBOS_SQS.createSQS(awscfg);
        try {
          await DBOS_SQS.validateConnection(sqs, url);
          executor.logger.info(`Successfully connected to SQS queue ${url} for ${cname}.${mname}`);
        } catch (e) {
          const err = e as Error;
          executor.logger.error(err);
          throw new DBOSError.DBOSError(
            `SQS Receiver for ${cname}.${mname} was unable to connect to ${url}: ${err.message}`,
          );
        }
        this.listeners.push(
          (async (client: SQSClient, url: string) => {
            const executor = this.executor;
            if (!executor) return;

            while (!this.isShuttingDown) {
              // Get message
              try {
                const response = await SQSReceiver.sendMessageSafe(
                  client,
                  new ReceiveMessageCommand({
                    QueueUrl: url,
                    MaxNumberOfMessages: 1,
                    WaitTimeSeconds: 5,
                  }),
                );

                if (!response.Messages || response.Messages.length === 0) {
                  executor.logger.debug(`No messages for ${url} - `);
                  continue;
                }

                const message = response.Messages[0];

                // Start workflow
                let wfid = mro.config?.getWFKey ? mro.config.getWFKey(message) : undefined;
                if (!wfid) {
                  wfid = cro.config?.getWFKey ? cro.config.getWFKey(message) : undefined;
                }
                if (!wfid) wfid = message.MessageId;
                await executor.workflow(
                  method.registeredFunction as unknown as WorkflowFunction<unknown[], unknown>,
                  { workflowUUID: wfid, queueName: mro.config?.workflowQueueName ?? cro.config?.workflowQueueName },
                  message,
                );

                // Delete the message from the queue after starting workflow (which will take over retries)
                await client.send(
                  new DeleteMessageCommand({
                    QueueUrl: url,
                    ReceiptHandle: message.ReceiptHandle,
                  }),
                );
              } catch (e) {
                DBOS.logger.error(e);
              }
            }
          })(sqs, url),
        );
      }
    }
  }

  logRegisteredEndpoints() {
    if (!this.executor) return;
    DBOS.logger.info('SQS receiver endpoints:');
    const regops = this.executor.getRegistrationsFor(this);
    regops.forEach((registeredOperation) => {
      const co = registeredOperation.classConfig as SQSReceiverClassDefaults;
      const mo = registeredOperation.methodConfig as SQSReceiverMethodSpecifics;
      const url = co.config?.queueUrl ?? co.config?.queueURL ?? mo.config?.queueUrl ?? mo.config?.queueURL;
      if (url) {
        const cname = registeredOperation.methodReg.className;
        const mname = registeredOperation.methodReg.name;
        DBOS.logger.info(`    ${url} -> ${cname}.${mname}`);
      }
    });
  }
}

let sqsRcv: SQSReceiver | undefined = undefined;

// Decorators - class
function SQSConfigure(config: SQSConfig) {
  function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
    if (!sqsRcv) sqsRcv = new SQSReceiver();
    const erInfo = associateClassWithEventReceiver(sqsRcv, ctor) as SQSReceiverClassDefaults;
    erInfo.config = config;
  }
  return clsdec;
}

// Decorators - method
function SQSMessageConsumer(config?: SQSConfig) {
  function mtddec<This, Args extends [Message] | [WorkflowContext, Message], Return>(
    target: object,
    propertyKey: string,
    inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
  ) {
    if (!sqsRcv) sqsRcv = new SQSReceiver();

    const { descriptor, receiverInfo } = associateMethodWithEventReceiver(sqsRcv, target, propertyKey, inDescriptor);

    const mRegistration = receiverInfo as SQSReceiverMethodSpecifics;
    mRegistration.config = config;

    return descriptor;
  }
  return mtddec;
}

export {
  SQSConfig,
  DBOS_SQS,
  SQSCommunicator,
  SQSCommunicator as SQSStep,
  SQSConfigure,
  SQSMessageConsumer,
  SQSReceiver,
};
