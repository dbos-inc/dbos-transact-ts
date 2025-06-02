import * as crypto from 'crypto';

import {
  ArgDataType,
  associateParameterWithExternal,
  DBOSDataType,
  DBOSMethodMiddlewareInstaller,
  MethodParameter,
  registerMiddlewareInserter,
} from './decorators';

import { MethodRegistrationBase } from './decorators';
import { DBOSDataValidationError } from './error';

import { DBOS } from './dbos';

const VALIDATOR = 'validator';

export enum ArgRequiredOptions {
  REQUIRED = 'REQUIRED',
  OPTIONAL = 'OPTIONAL',
  DEFAULT = 'DEFAULT',
}

interface ValidatorClassInfo {
  defaultArgRequired?: ArgRequiredOptions;
  defaultArgValidate?: boolean;
}

interface ValidatorFuncInfo {
  performArgValidation?: boolean;
}

interface ValidatorArgInfo {
  required?: ArgRequiredOptions;
}

function getValidatorClassInfo(methReg: MethodRegistrationBase) {
  const valInfo = methReg.defaults?.getRegisteredInfo(VALIDATOR) as ValidatorClassInfo;
  return {
    defaultArgRequired: valInfo?.defaultArgRequired ?? ArgRequiredOptions.DEFAULT,
    defaultArgValidate: valInfo?.defaultArgValidate ?? false,
  } satisfies ValidatorClassInfo;
}

export function requestArgValidation(methReg: MethodRegistrationBase) {
  (methReg.getRegisteredInfo(VALIDATOR) as ValidatorFuncInfo).performArgValidation = true;
}

function getValidatorFuncInfo(methReg: MethodRegistrationBase) {
  const valInfo = methReg.getRegisteredInfo(VALIDATOR) as ValidatorFuncInfo;
  return {
    performArgValidation: valInfo.performArgValidation ?? false,
  } satisfies ValidatorFuncInfo;
}

function getValidatorArgInfo(param: MethodParameter) {
  const valInfo = param.getRegisteredInfo(VALIDATOR) as ValidatorArgInfo;
  return {
    required: valInfo.required ?? ArgRequiredOptions.DEFAULT,
  };
}

class ValidationInserter implements DBOSMethodMiddlewareInstaller {
  installMiddleware(methReg: MethodRegistrationBase): void {
    const valInfo = getValidatorClassInfo(methReg);
    const defaultArgRequired = valInfo.defaultArgRequired;
    const defaultArgValidate = valInfo.defaultArgValidate;

    let shouldValidate =
      getValidatorFuncInfo(methReg).performArgValidation ||
      defaultArgRequired === ArgRequiredOptions.REQUIRED ||
      defaultArgValidate;

    for (const a of methReg.args) {
      if (getValidatorArgInfo(a).required === ArgRequiredOptions.REQUIRED) {
        shouldValidate = true;
      }
    }

    if (shouldValidate) {
      requestArgValidation(methReg);
      methReg.addEntryInterceptor(validateMethodArgs, 20);
    }
  }
}

const valInserter = new ValidationInserter();

export function ArgRequired(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const curParam = associateParameterWithExternal(
    VALIDATOR,
    target,
    undefined,
    propertyKey.toString(),
    undefined,
    parameterIndex,
  ) as ValidatorArgInfo;

  curParam.required = ArgRequiredOptions.REQUIRED;

  registerMiddlewareInserter(valInserter);
}

export function ArgOptional(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const curParam = associateParameterWithExternal(
    VALIDATOR,
    target,
    undefined,
    propertyKey.toString(),
    undefined,
    parameterIndex,
  ) as ValidatorArgInfo;

  curParam.required = ArgRequiredOptions.OPTIONAL;

  registerMiddlewareInserter(valInserter);
}

export function ArgDate() {
  // TODO a little more info about it - is it a date or timestamp precision?
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const curParam = associateParameterWithExternal(
      'type',
      target,
      undefined,
      propertyKey.toString(),
      undefined,
      parameterIndex,
    ) as ArgDataType;

    if (!curParam.dataType) curParam.dataType = new DBOSDataType();
    curParam.dataType.dataType = 'timestamp';

    registerMiddlewareInserter(valInserter);
  };
}

export function ArgVarchar(length: number) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const curParam = associateParameterWithExternal(
      'type',
      target,
      undefined,
      propertyKey.toString(),
      undefined,
      parameterIndex,
    ) as ArgDataType;

    curParam.dataType = DBOSDataType.varchar(length);

    registerMiddlewareInserter(valInserter);
  };
}

export function DefaultArgRequired<T extends { new (...args: unknown[]): object }>(ctor: T) {
  const clsreg = DBOS.associateClassWithInfo(VALIDATOR, ctor) as ValidatorClassInfo;
  clsreg.defaultArgRequired = ArgRequiredOptions.REQUIRED;

  registerMiddlewareInserter(valInserter);
}

export function DefaultArgValidate<T extends { new (...args: unknown[]): object }>(ctor: T) {
  const clsreg = DBOS.associateClassWithInfo(VALIDATOR, ctor) as ValidatorClassInfo;
  clsreg.defaultArgValidate = true;

  registerMiddlewareInserter(valInserter);
}

export function DefaultArgOptional<T extends { new (...args: unknown[]): object }>(ctor: T) {
  const clsreg = DBOS.associateClassWithInfo(VALIDATOR, ctor) as ValidatorClassInfo;
  clsreg.defaultArgRequired = ArgRequiredOptions.OPTIONAL;

  registerMiddlewareInserter(valInserter);
}

export enum LogMasks {
  NONE = 'NONE',
  HASH = 'HASH',
  SKIP = 'SKIP',
}

interface LoggerArgInfo {
  logMask?: LogMasks;
}

export const LOGGER = 'log';

export function SkipLogging(target: object, propertyKey: string | symbol, parameterIndex: number) {
  const curParam = associateParameterWithExternal(
    LOGGER,
    target,
    undefined,
    propertyKey.toString(),
    undefined,
    parameterIndex,
  ) as LoggerArgInfo;

  curParam.logMask = LogMasks.SKIP;
}

export function LogMask(mask: LogMasks) {
  return function (target: object, propertyKey: string | symbol, parameterIndex: number) {
    const curParam = associateParameterWithExternal(
      LOGGER,
      target,
      undefined,
      propertyKey.toString(),
      undefined,
      parameterIndex,
    ) as LoggerArgInfo;

    curParam.logMask = mask;
  };
}

function generateSaltedHash(data: string, salt: string): string {
  const hash = crypto.createHash('sha256'); // You can use other algorithms like 'md5', 'sha512', etc.
  hash.update(data + salt);
  return hash.digest('hex');
}

function getLoggerArgInfo(param: MethodParameter) {
  const valInfo = param.getRegisteredInfo(LOGGER) as LoggerArgInfo;
  return {
    logMask: valInfo.logMask ?? LogMasks.NONE,
  };
}

class LoggingInserter implements DBOSMethodMiddlewareInstaller {
  installMiddleware(methReg: MethodRegistrationBase): void {
    methReg.addEntryInterceptor(logMethodArgs, 30);
  }
}

const logInserter = new LoggingInserter();
registerMiddlewareInserter(logInserter);

export function logMethodArgs<Args extends unknown[]>(methReg: MethodRegistrationBase, args: Args) {
  // Argument logging
  args.forEach((argValue, idx) => {
    if (idx === 0 && methReg.passContext) {
      return;
    }

    let loggedArgValue = argValue;
    const logMask = getLoggerArgInfo(methReg.args[idx]).logMask;

    if (logMask === LogMasks.SKIP) {
      return;
    } else {
      if (logMask !== LogMasks.NONE) {
        // For now this means hash
        if (methReg.args[idx].dataType?.dataType === 'json') {
          loggedArgValue = generateSaltedHash(JSON.stringify(argValue), 'JSONSALT');
        } else {
          // Yes, we are doing the same as above for now.
          // It can be better if we have verified the type of the data
          loggedArgValue = generateSaltedHash(JSON.stringify(argValue), 'DBOSSALT');
        }
      }
      DBOS.span?.setAttribute(methReg.args[idx].name, loggedArgValue as string);
    }
  });

  return args;
}

function validateMethodArgs<Args extends unknown[]>(methReg: MethodRegistrationBase, args: Args) {
  const validationError = (msg: string) => {
    const err = new DBOSDataValidationError(msg);
    DBOS.span?.addEvent('DataValidationError', { message: err.message });
    return err;
  };

  // Input validation
  methReg.args.forEach((argDescriptor, idx) => {
    if (idx === 0 && methReg.passContext) {
      return;
    }

    let argValue = args[idx];

    // So... there is such a thing as "undefined", and another thing called "null"
    // We will fold this to "undefined" for our APIs.  It's just a rule of ours.
    if (argValue === null) {
      argValue = undefined;
      args[idx] = undefined;
    }

    if (argValue === undefined) {
      const valInfo = getValidatorClassInfo(methReg);
      const defaultArgRequired = valInfo.defaultArgRequired;
      const defaultArgValidate = valInfo.defaultArgValidate;
      const argRequired = getValidatorArgInfo(argDescriptor).required;
      if (
        argRequired === ArgRequiredOptions.REQUIRED ||
        (argRequired === ArgRequiredOptions.DEFAULT &&
          (defaultArgRequired === ArgRequiredOptions.REQUIRED || defaultArgValidate))
      ) {
        if (idx >= args.length) {
          throw validationError(
            `Insufficient number of arguments calling ${methReg.name} - ${args.length}/${methReg.args.length}`,
          );
        } else {
          throw validationError(`Missing required argument ${argDescriptor.name} of ${methReg.name}`);
        }
      }
    }

    if (argValue === undefined) {
      return;
    }

    if (argValue instanceof String) {
      argValue = argValue.toString();
      args[idx] = argValue;
    }
    if (argValue instanceof Boolean) {
      argValue = argValue.valueOf();
      args[idx] = argValue;
    }
    if (argValue instanceof Number) {
      argValue = argValue.valueOf();
      args[idx] = argValue;
    }
    if (argValue instanceof BigInt) {
      // ES2020+
      argValue = argValue.valueOf();
      args[idx] = argValue;
    }

    // Argument validation - below - if we have any info about it
    if (!argDescriptor.dataType) return;

    // Maybe look into https://www.npmjs.com/package/validator
    //  We could support emails and other validations too with something like that...
    if (argDescriptor.dataType.dataType === 'text' || argDescriptor.dataType.dataType === 'varchar') {
      if (typeof argValue !== 'string') {
        throw validationError(
          `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a string`,
        );
      }
      if (argDescriptor.dataType.length > 0) {
        if (argValue.length > argDescriptor.dataType.length) {
          throw validationError(
            `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' with maximum length ${argDescriptor.dataType.length} but has length ${argValue.length}`,
          );
        }
      }
    }
    if (argDescriptor.dataType.dataType === 'boolean') {
      if (typeof argValue !== 'boolean') {
        if (typeof argValue === 'number') {
          if (argValue === 0 || argValue === 1) {
            argValue = argValue !== 0 ? true : false;
            args[idx] = argValue;
          } else {
            throw validationError(
              `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and may be a number (0 or 1) convertible to boolean, but was ${argValue}.`,
            );
          }
        } else if (typeof argValue === 'string') {
          if (argValue.toLowerCase() === 't' || argValue.toLowerCase() === 'true' || argValue === '1') {
            argValue = true;
            args[idx] = argValue;
          } else if (argValue.toLowerCase() === 'f' || argValue.toLowerCase() === 'false' || argValue === '0') {
            argValue = false;
            args[idx] = argValue;
          } else {
            throw validationError(
              `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and may be a string convertible to boolean, but was ${argValue}.`,
            );
          }
        } else {
          throw validationError(
            `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a boolean`,
          );
        }
      }
    }
    if (argDescriptor.dataType.dataType === 'decimal') {
      // Range check precision and scale... wishing there was a bigdecimal
      //  Floats don't really permit us to check the scale.
      if (typeof argValue !== 'number') {
        throw validationError(
          `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a number`,
        );
      }
      let prec = argDescriptor.dataType.precision;
      if (prec > 0) {
        if (argDescriptor.dataType.scale > 0) {
          prec = prec - argDescriptor.dataType.scale;
        }
        if (Math.abs(argValue) >= Math.exp(prec)) {
          throw validationError(
            `Argument ${argDescriptor.name} of ${methReg.name} is out of range for type '${argDescriptor.dataType.formatAsString()}`,
          );
        }
      }
    }
    if (argDescriptor.dataType.dataType === 'double' || argDescriptor.dataType.dataType === 'integer') {
      if (typeof argValue !== 'number') {
        if (typeof argValue === 'string') {
          const n = parseFloat(argValue);
          if (isNaN(n)) {
            throw validationError(
              `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a number`,
            );
          }
          argValue = n;
          args[idx] = argValue;
        } else if (typeof argValue === 'bigint') {
          // Hum, maybe we should allow bigint as a type, number won't even do 64-bit.
          argValue = Number(argValue).valueOf();
          args[idx] = argValue;
        } else {
          throw validationError(
            `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' and should be a number`,
          );
        }
      }
      if (argDescriptor.dataType.dataType === 'integer') {
        if (!Number.isInteger(argValue)) {
          throw validationError(
            `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' but has a fractional part`,
          );
        }
      }
    }
    if (argDescriptor.dataType.dataType === 'timestamp') {
      if (!(argValue instanceof Date)) {
        if (typeof argValue === 'string') {
          const d = Date.parse(argValue);
          if (isNaN(d)) {
            throw validationError(
              `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' but is a string that will not parse as Date`,
            );
          }
          argValue = new Date(d);
          args[idx] = argValue;
        } else {
          throw validationError(
            `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' but is not a date or time`,
          );
        }
      }
    }
    if (argDescriptor.dataType.dataType === 'uuid') {
      // This validation is loose.  A tighter one would be:
      // /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$/
      // That matches UUID version 1-5.
      if (!/^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(String(argValue))) {
        throw validationError(
          `Argument ${argDescriptor.name} of ${methReg.name} is marked as type '${argDescriptor.dataType.dataType}' but is not a valid UUID`,
        );
      }
    }
    // JSON can be anything.  We can validate it against a schema at some later version...
  });
  return args;
}
