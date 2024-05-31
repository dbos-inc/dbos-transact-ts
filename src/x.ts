class WFContext {
    okey: string = '';
}

class ConfiguredInstance {
    // This interface can have other properties/methods as needed
}

type MethodWithWFContext<T extends ConfiguredInstance> = {
    [K in keyof T]: T[K] extends (context: WFContext, ...args: any[]) => any ? K : never;
}[keyof T];

type MethodArguments<T extends ConfiguredInstance, K extends keyof T> = T[K] extends (context: WFContext, ...args: infer A) => any ? A : never;

class C
{
    context: WFContext = new WFContext();

    callConfiguredMethod<
    T extends ConfiguredInstance,
    K extends MethodWithWFContext<T>
>(
    instance: T,
    methodName: K,
    ...args: MethodArguments<T, K>
): T[K] extends (context: WFContext, ...args: any[]) => infer R ? R : never {
    const method = instance[methodName];
    if (typeof method !== 'function') {
        throw new Error(`Method ${String(methodName)} is not a function`);
    }
    return method.call(instance, this.context, ...args);
}
}
const c = new C();

// Example class implementing ConfiguredInstance with methods having WFContext as the first parameter
class MyConfiguredClass implements ConfiguredInstance {
    someMethod(context: WFContext, arg1: string, arg2: number): void {
        console.log(context.okey, arg1, arg2);
    }

    anotherMethod(context: WFContext, arg: boolean): number {
        console.log(context.okey, arg);
        return 42;
    }

    anotherAnother(this: MyConfiguredClass, context: WFContext, arg1: string): string {
        c.callConfiguredMethod(this, 'anotherMethod', true);
        return "yay";	
    }
}

// Example usage
const instance = new MyConfiguredClass();
//const context: WFContext = { okey: 'example' };

// Call someMethod
c.callConfiguredMethod(instance, 'someMethod', 'hello', 123);

// Call anotherMethod
const result = c.callConfiguredMethod(instance, 'anotherMethod', true);
console.log(result); // 42

