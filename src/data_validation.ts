import { MethodRegistrationBase, ArgRequiredOptions } from "./decorators";
import { DBOSContextImpl, getCurrentDBOSContext } from "./context";
import { DBOSDataValidationError } from "./error";

export function validateMethodArgs<Args extends unknown[]>(methReg: MethodRegistrationBase, args: Args)
{
    let opCtx : DBOSContextImpl | undefined = undefined;
    if (!methReg.passContext) {
        opCtx = getCurrentDBOSContext() as DBOSContextImpl;
    }

    const validationError = (msg:string) => {
        const err = new DBOSDataValidationError(msg);
        opCtx?.span?.addEvent("DataValidationError", { message: err.message });
        return err;
    }

    // Input validation
    methReg.args.forEach((argDescriptor, idx) => {
        if (idx === 0 && methReg.passContext)
        {
          // Context, may find a more robust way.
          opCtx = args[idx] as DBOSContextImpl;
          return;
        }

        if (!methReg.performArgValidation && !methReg.defaults?.defaultArgValidate && argDescriptor.required !== ArgRequiredOptions.REQUIRED) {
          return;
        }

        // Do we have an arg at all
        if (idx >= args.length) {
          if (argDescriptor.required === ArgRequiredOptions.REQUIRED ||
             argDescriptor.required === ArgRequiredOptions.DEFAULT && methReg.defaults?.defaultArgRequired === ArgRequiredOptions.REQUIRED)
          {
            throw validationError(`Insufficient number of arguments calling ${methReg.name} - ${args.length}/${methReg.args.length}`);
          }
          return;
        }

        let argValue = args[idx];

        // So... there is such a thing as "undefined", and another thing called "null"
        // We will fold this to "undefined" for our APIs.  It's just a rule of ours.
        if (argValue === null) {
          argValue = undefined;
          args[idx] = undefined;
        }

        if (argValue === undefined && (argDescriptor.required === ArgRequiredOptions.REQUIRED ||
          (argDescriptor.required === ArgRequiredOptions.DEFAULT && methReg.defaults?.defaultArgRequired === ArgRequiredOptions.REQUIRED)))
        {
          throw validationError(`Missing required argument ${argDescriptor.name} of ${methReg.name}`);
        }
        if (argValue === undefined) {
          return;
        }

        if (argValue instanceof String) {
          argValue = argValue.toString();
          args[idx] = argValue;
        }
        if (argValue instanceof Boolean) {
          argValue = argValue.valueOf()
          args[idx] = argValue;
        }
        if (argValue instanceof Number) {
          argValue = argValue.valueOf()
          args[idx] = argValue;
        }
        if (argValue instanceof BigInt) { // ES2020+
          argValue = argValue.valueOf()
          args[idx] = argValue;
        }

        // Maybe look into https://www.npmjs.com/package/validator
        //  We could support emails and other validations too with something like that...
        if (argDescriptor.dataType.dataType === 'text' || argDescriptor.dataType.dataType === 'varchar') {
          if ((typeof argValue !== 'string')) {
            throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a string`);
          }
          if (argDescriptor.dataType.length > 0) {
            if (argValue.length > argDescriptor.dataType.length) {
              throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' with maximum length ${argDescriptor.dataType.length} but has length ${argValue.length}`);
            }
          }
        }
        if (argDescriptor.dataType.dataType === 'boolean') {
          if (typeof argValue !== 'boolean') {
            if (typeof argValue === 'number') {
              if (argValue === 0 || argValue === 1) {
                argValue = (argValue !== 0 ? true : false);
                args[idx] = argValue;
              }
              else {
                throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and may be a number (0 or 1) convertible to boolean, but was ${argValue}.`);
              }
            }
            else if (typeof argValue === 'string') {
              if (argValue.toLowerCase() === 't' || argValue.toLowerCase() === 'true' || argValue === '1') {
                argValue = true;
                args[idx] = argValue;
              }
              else if (argValue.toLowerCase() === 'f' || argValue.toLowerCase() === 'false' || argValue === '0') {
                argValue = false;
                args[idx] = argValue;
              }
              else {
                throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and may be a string convertible to boolean, but was ${argValue}.`);
              }
            }
            else {
              throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a boolean`);
            }
          }
        }
        if (argDescriptor.dataType.dataType === 'decimal') {
          // Range check precision and scale... wishing there was a bigdecimal
          //  Floats don't really permit us to check the scale.
          if (typeof argValue !== 'number') {
            throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a number`);
          }
          let prec = argDescriptor.dataType.precision;
          if (prec > 0) {
            if (argDescriptor.dataType.scale > 0) {
              prec = prec - argDescriptor.dataType.scale;
            }
            if (Math.abs(argValue) >= Math.exp(prec)) {
              throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is out of range for type '${argDescriptor.dataType.formatAsString()}`);
            }
          }
        }
        if (argDescriptor.dataType.dataType === 'double' || argDescriptor.dataType.dataType === 'integer') {
          if (typeof argValue !== 'number') {
            if (typeof argValue === 'string') {
              const n = parseFloat(argValue);
              if (isNaN(n)) {
                throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a number`);
              }
              argValue = n;
              args[idx] = argValue;
            }
            else if (typeof argValue === 'bigint') {
              // Hum, maybe we should allow bigint as a type, number won't even do 64-bit.
              argValue = Number(argValue).valueOf();
              args[idx] = argValue;
            }
            else {
              throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a number`);
            }
          }
          if (argDescriptor.dataType.dataType === 'integer') {
            if (!Number.isInteger(argValue)) {
              throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' but has a fractional part`);
            }
          }
        }
        if (argDescriptor.dataType.dataType === 'timestamp') {
          if (!(argValue instanceof Date)) {
            if (typeof argValue === 'string') {
              const d = Date.parse(argValue);
              if (isNaN(d)) {
                throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' but is a string that will not parse as Date`);
              }
              argValue = new Date(d);
              args[idx] = argValue;
            }
            else {
              throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' but is not a date or time`);
            }
          }
        }
        if (argDescriptor.dataType.dataType === 'uuid') {
          // This validation is loose.  A tighter one would be:
          // /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/
          // That matches UUID version 1-5.
          if (!/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(String(argValue))) {
            throw validationError(`Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' but is not a valid UUID`);
          }
        }
        // JSON can be anything.  We can validate it against a schema at some later version...
    });
    return args;
}
