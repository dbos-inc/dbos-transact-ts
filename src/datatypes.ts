enum LogMask {
  NONE = "NONE",
  HASH = "HASH",
}

type OperonFieldType =
  | "integer"
  | "double"
  | "decimal"
  | "timestamp"
  | "text"
  | "varchar"
  | "boolean"
  | "uuid"
  | "json";

class OperonDataType {
  dataType: OperonFieldType = "text";
  length: number = -1;
  precision: number = -1;
  scale: number = -1;

  /** Varchar has length */
  static varchar(length: number) {
    const dt = new OperonDataType();
    dt.dataType = "varchar";
    dt.length = length;
    return dt;
  }

  /** Some decimal has precision / scale (as opposed to floating point decimal) */
  static decimal(precision: number, scale: number) {
    const dt = new OperonDataType();
    dt.dataType = "decimal";
    dt.precision = precision;
    dt.scale = scale;

    return dt;
  }

  /** Take type from reflect metadata */
  static fromArg(arg: Function) {
    const dt = new OperonDataType();

    if (arg === String) {
      dt.dataType = "text";
    } else if (arg === Date) {
      dt.dataType = "timestamp";
    } else if (arg === Number) {
      dt.dataType = "double";
    } else if (arg === Boolean) {
      dt.dataType = "boolean";
    } else {
      dt.dataType = "json";
    }

    return dt;
  }

  formatAsString(): string {
    let rv: string = this.dataType;
    if (this.dataType === "varchar" && this.length > 0) {
      rv += `(${this.length})`;
    }
    if (this.dataType === "decimal" && this.precision > 0) {
      if (this.scale > 0) {
        rv += `(${this.precision},${this.scale})`;
      } else {
        rv += `(${this.precision})`;
      }
    }
    return rv;
  }
}

class OperonParameter {
  name: string = "";
  required: boolean = false;
  validate: boolean = true;
  skipLogging: boolean = false;
  logMask: LogMask = LogMask.NONE;
  argType: Function = String;
  dataType: OperonDataType;
  // TODO: If we override the logging behavior (say to mask/hash it), where do we record that?
  index: number = -1;

  constructor(idx: number, name: string) {
    this.index = idx;
    this.name = name;
    this.dataType = OperonDataType.varchar(32);
  }
}

export class OperonMethodRegistrationBase {
  name: string = "";
  args: OperonParameter[] = [];

  constructor(name: string, args: string[]) {
    this.name = name;
    for (let i = 0; i < args.length; i++) {
      this.args.push(new OperonParameter(i, args[i]));
    }
  }
}
