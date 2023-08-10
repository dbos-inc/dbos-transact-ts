export class OperationContext {
    constructor(
        // Registration parameters
        protected readonly workflowName: string,
        protected readonly rolesThatCanRun: string[] = [],

        // Operation instance parameters
        protected readonly workflowUUID: string,
        protected readonly runAs: string,
    ) { }
}
