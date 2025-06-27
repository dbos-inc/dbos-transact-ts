import { DBOS, Error as DBOSError, DBOSLifecycleCallback } from '@dbos-inc/dbos-sdk';

import {
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  GetQueueAttributesCommandInput,
  Message,
  ReceiveMessageCommand,
  ReceiveMessageCommandOutput,
  SQSClient,
} from '@aws-sdk/client-sqs';

interface SQSConfig {
  client?: SQSClient | (() => SQSClient);
  queueUrl?: string;
  getWorkflowKey?: (m: Message) => string;
  workflowQueueName?: string;
}

interface SQSRecvClassConfig {
  config?: SQSConfig;
}

interface SQSRecvMethodConfig {
  config?: SQSConfig;
}

class SQSReceiver extends DBOSLifecycleCallback {
  config?: SQSConfig;

  constructor(config?: SQSConfig) {
    super();
    this.config = config;
  }

  listeners: Promise<void>[] = [];
  isShuttingDown = false;

  override async destroy() {
    this.isShuttingDown = true;
    try {
      await Promise.allSettled(this.listeners);
    } catch (e) {
      // yawn
    }
    this.listeners = [];
  }

  // async function that uses .then/.catch to handle potentially unreliable library calls
  static async sendReceiveMessageCommandSafe(
    sqs: SQSClient,
    params: ReceiveMessageCommand,
  ): Promise<ReceiveMessageCommandOutput> {
    return new Promise((resolve, reject) => {
      sqs
        .send(params)
        .then((response) => resolve(response))
        .catch((error) => reject(error as Error));
    });
  }

  static async validateConnection(client: SQSClient, url: string) {
    const params: GetQueueAttributesCommandInput = {
      QueueUrl: url,
      AttributeNames: ['All'],
    };

    const _validateSQSConfiguration = await client.send(new GetQueueAttributesCommand(params));
  }

  override async initialize() {
    const regops = DBOS.getAssociatedInfo(this);

    for (const regop of regops) {
      const { methodConfig, classConfig, methodReg } = regop;
      const sqsclass = classConfig as SQSRecvClassConfig;
      const sqsmethod = methodConfig as SQSRecvMethodConfig;
      if (!sqsmethod) continue;

      const url = sqsclass.config?.queueUrl ?? sqsmethod.config?.queueUrl ?? this.config?.queueUrl;
      if (!url) continue;

      const cname = methodReg.className;
      const mname = methodReg.name;

      if (!methodReg.workflowConfig) {
        throw new DBOSError.DBOSError(
          `Error registering method ${cname}.${mname}: An SQS decorator can only be assigned to a workflow!`,
        );
      }

      let sqs: SQSClient | undefined = undefined;
      if (sqsmethod.config?.client) {
        sqs = typeof sqsmethod.config.client === 'function' ? sqsmethod.config.client() : sqsmethod.config.client;
      } else if (sqsclass.config?.client) {
        sqs = typeof sqsclass.config.client === 'function' ? sqsclass.config.client() : sqsclass.config.client;
      } else if (this.config?.client) {
        sqs = typeof this.config.client === 'function' ? this.config.client() : this.config.client;
      }

      if (!sqs) {
        throw new DBOSError.DBOSError(`Error initializing SQS method ${cname}.${mname}: No SQSClient provided`);
      }
      try {
        await SQSReceiver.validateConnection(sqs, url);
        DBOS.logger.info(`Successfully connected to SQS queue ${url} for ${cname}.${mname}`);
      } catch (e) {
        const err = e as Error;
        DBOS.logger.error(err);
        throw new DBOSError.DBOSError(
          `SQS Receiver for ${cname}.${mname} was unable to connect to ${url}: ${err.message}`,
        );
      }

      // eslint-disable-next-line @typescript-eslint/no-this-alias
      const sqsr = this;
      this.listeners.push(
        (async (client: SQSClient, url: string) => {
          while (!this.isShuttingDown) {
            // Get message
            try {
              const response = await SQSReceiver.sendReceiveMessageCommandSafe(
                client,
                new ReceiveMessageCommand({
                  QueueUrl: url,
                  MaxNumberOfMessages: 1,
                  WaitTimeSeconds: 5,
                }),
              );

              if (!response.Messages || response.Messages.length === 0) {
                DBOS.logger.debug(`No messages for ${url} - `);
                continue;
              }

              const message = response.Messages[0];

              // Start workflow
              let wfid = sqsmethod.config?.getWorkflowKey ? sqsmethod.config.getWorkflowKey(message) : undefined;
              if (!wfid) {
                wfid = sqsclass.config?.getWorkflowKey ? sqsclass.config.getWorkflowKey(message) : undefined;
              }
              if (!wfid) {
                wfid = sqsr.config?.getWorkflowKey ? sqsr.config.getWorkflowKey(message) : undefined;
              }
              if (!wfid) wfid = message.MessageId;
              await DBOS.startWorkflow(methodReg.registeredFunction as (...args: unknown[]) => Promise<unknown>, {
                workflowID: wfid,
                queueName:
                  sqsmethod.config?.workflowQueueName ??
                  sqsclass.config?.workflowQueueName ??
                  sqsr.config?.workflowQueueName,
              })(message);

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

  override logRegisteredEndpoints() {
    DBOS.logger.info('SQS receiver endpoints:');

    const eps = DBOS.getAssociatedInfo(this);

    for (const e of eps) {
      const { methodConfig, classConfig, methodReg } = e;
      const sqsclass = classConfig as SQSRecvClassConfig;
      const sqsmethod = methodConfig as SQSRecvMethodConfig;
      if (sqsmethod) {
        const url = sqsclass.config?.queueUrl ?? sqsmethod.config?.queueUrl ?? this.config?.queueUrl;
        if (url) {
          const cname = methodReg.className;
          const mname = methodReg.name;
          DBOS.logger.info(`    ${url} -> ${cname}.${mname}`);
        }
      }
    }
  }

  // Decorator - class
  configuration(config: SQSConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;

    function clsdec<T extends { new (...args: unknown[]): object }>(ctor: T) {
      const erInfo = DBOS.associateClassWithInfo(er, ctor) as SQSRecvClassConfig;
      erInfo.config = config;
    }
    return clsdec;
  }

  // Decorators - method
  messageConsumer(config?: SQSConfig) {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const er = this;
    function mtddec<This, Args extends [Message], Return>(
      target: object,
      propertyKey: string,
      inDescriptor: TypedPropertyDescriptor<(this: This, ...args: Args) => Promise<Return>>,
    ) {
      const { regInfo: receiverInfo } = DBOS.associateFunctionWithInfo(er, inDescriptor.value!, {
        ctorOrProto: target,
        name: propertyKey,
      });

      const mRegistration = receiverInfo as SQSRecvMethodConfig;
      mRegistration.config = config;

      return inDescriptor;
    }
    return mtddec;
  }
}

export { SQSConfig, SQSReceiver };
