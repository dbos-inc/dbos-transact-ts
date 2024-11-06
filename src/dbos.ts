import { getCurrentDBOSContext } from "./context";
import { DBOSExecutor } from "./dbos-executor";
import { DBOSExecutorContext } from "./eventreceiver";
import { DLogger, GlobalLogger } from "./telemetry/logs";

export class DBOS
{
    ///////
    // Lifecycle
    ///////
    static launch() {
    }

    static get executor() {
        return DBOSExecutor.globalInstance as DBOSExecutorContext;
    }

    //////
    // Context
    //////
    static get logger() : DLogger {
        const ctx = getCurrentDBOSContext();
        if (ctx) return ctx.logger;
        const executor = DBOS.executor;
        if (executor) return executor.logger;
        return new GlobalLogger();
    }

    // TODO Auth user

    //////
    // Decorators
    //////
}