import { ConfiguredClass, InitConfigMethod } from './decorators';

export {
  createTestingRuntime,
  TestingRuntime,
} from './testing/testing_runtime';

export {
  DBOSContext,
  InitContext,
} from './context';

export {
  TransactionContext,
  TransactionConfig,
} from './transaction';

export {
  WorkflowContext,
  WorkflowConfig,
  WorkflowHandle,
  StatusString,
} from './workflow';

export {
  CommunicatorContext,
  CommunicatorConfig,
} from './communicator';

export * as Error from './error';

export {
  DBOSResponseError
} from './error';

export {
  LogMasks,

  // Parameter Decorators
  ArgRequired,
  ArgOptional,
  SkipLogging,
  LogMask,
  ArgName,
  ArgDate,
  ArgVarchar,

  // Class Decorators
  Configurable,
    initClassConfiguration,
    ConfiguredClass,
  DefaultRequiredRole,
  DefaultArgRequired,
  DefaultArgOptional,
  // Typeorm Class Decorators
  OrmEntities,

  // Method Decorators
  Transaction,
  Workflow,
  Communicator,
  RequiredRole,
  DBOSInitializer,
  DBOSDeploy,
} from "./decorators";

export type ConfiguredClassType<C extends InitConfigMethod> = ConfiguredClass<C, Parameters<C['initConfiguration']>[1]>;

export {
  ArgSources,
} from "./httpServer/handlerTypes";

export {
  HandlerContext,

  // Endpoint Parameter Decorators
  ArgSource,

  // Endpoint Decorators
  GetApi,
  PostApi,
} from "./httpServer/handler";

export {
  DBOSHttpAuthMiddleware,
  DBOSHttpAuthReturn,
  MiddlewareContext,

  // Middleware Decorators
  Authentication,
  KoaBodyParser,
  KoaCors,
  KoaMiddleware,

  // OpenApi Decorators
  OpenApiSecurityScheme
} from "./httpServer/middleware";

export {
  Kafka,
  KafkaConsume,
} from "./kafka/kafka";

export {
  SchedulerMode,
  SchedulerConfig,
  Scheduled,
} from "./scheduler/scheduler";
