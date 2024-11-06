import { Span } from "@opentelemetry/sdk-trace-base";
import { getCurrentDBOSContext, HTTPRequest } from "./context";
import { DBOSConfig, DBOSExecutor } from "./dbos-executor";
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
    // Globals
    //////
    static globalLogger?: DLogger;
    static dbosConfig?: DBOSConfig;

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
    static get span(): Span | undefined {
        const ctx = getCurrentDBOSContext();
        if (ctx) return ctx.span;
        return undefined;
    }

    static get request(): HTTPRequest | undefined {
        return getCurrentDBOSContext()?.request;
    }

    static get workflowID(): string | undefined {
        return getCurrentDBOSContext()?.workflowUUID;
    }
    static get authenticatedUser(): string {
        return (getCurrentDBOSContext()?.authenticatedUser) ?? '';
    }
    static get authenticatedRoles(): string[] {
        return (getCurrentDBOSContext()?.authenticatedRoles) ?? [];
    }
    static get assumedRole(): string {
        return (getCurrentDBOSContext()?.assumedRole) ?? '';
    }

    static getConfig<T>(key: string): T | undefined;
    static getConfig<T>(key: string, defaultValue: T): T;
    static getConfig<T>(key: string, defaultValue?: T): T | undefined {
        const ctx = getCurrentDBOSContext();
        if (ctx && defaultValue) return ctx.getConfig<T>(key, defaultValue);
        if (ctx) return ctx.getConfig<T>(key);
        if (DBOS.executor) return DBOS.executor.getConfig(key, defaultValue);
        return defaultValue;
    }

    //////
    // Workflow and other operations
    //////

    //////
    // Decorators
    //////
}