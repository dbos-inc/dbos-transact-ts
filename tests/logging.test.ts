import { LogMasks, ArgName, SkipLogging, LogMask, getRegisteredOperations } from '../src/decorators';

import { DBOSContextImpl } from '../src/context';
import { generateDBOSTestConfig, setUpDBOSTestDb } from './helpers';
import { DBOSExecutor } from '../src/dbos-executor';

class TestFunctions {
  static foo(
    _ctx: DBOSContextImpl,
    @LogMask(LogMasks.HASH) arg1: string,
    /*@ArgDate()*/ arg2: Date,
    @SkipLogging arg3: boolean,
    @ArgName('arg4') arg_should_be_4: number,
  ): Promise<string> {
    return Promise.resolve('stringvalue' + arg1 + arg2.toDateString() + arg3 + arg_should_be_4);
  }
}

// Quick way to go through the method registrations for the logger... obviously needs moved

// QnD quoting of identifiers - note that this may be DB specific and belongs with the logger itself.
function quoteSqlIdentifier(identifier: string): string {
  // Escape any embedded double quotes
  const escaped = identifier.replace(/"/g, '""');

  // Wrap the identifier in double quotes
  return `"${escaped}"`;
}

function quoteSqlString(value: string): string {
  // Escape any embedded single quotes
  const escaped = value.replace(/'/g, "''");

  // Wrap the string in single quotes
  return `'${escaped}'`;
}

// FIXME: this test relies on manually reading the console log and the code doesn't check for correctness.
describe('dbos-logging', () => {
  test('Decorators', async () => {
    const ops = getRegisteredOperations(TestFunctions);
    ops.forEach((m) => {
      // This is not how you build SQL obviously.  It is up to the collector to do it, schema evolution, etc.
      let cts = `CREATE PGTABLEISH ${quoteSqlIdentifier('dbos_log_' + m.name)} (\n`;
      // Method-specific fields
      m.args.forEach((element) => {
        if (element.logMask === LogMasks.SKIP) {
          return;
        }
        // NB not all types here may match the SQL string
        let ctype = element.dataType.formatAsString();
        if (element.logMask === LogMasks.HASH) {
          ctype = 'VARCHAR(64)';
        }
        cts += '  ' + quoteSqlIdentifier(element.name) + ' ' + ctype + ',\n';
      });

      // Generic fields
      cts += '  event_type varchar(100),\n';
      cts += '  auth_user uuid,\n';
      cts += '  auth_role varchar(100),\n';
      cts += '  evt_time TIMESTAMP,\n';
      cts += `  method_name varchar(100) default (${quoteSqlString(m.name)})\n`;
      cts += ');\n';
      console.log(cts);
    });

    await TestFunctions.foo(null as unknown as DBOSContextImpl, 'a', new Date(), false, 4);
  });

  test('forceConsole', async () => {
    const mockConsoleLog = jest.spyOn(console, 'log').mockImplementation((() => {}) as any);
    try {
      const $dbosConfig = generateDBOSTestConfig();
      const { telemetry } = $dbosConfig;
      const dbosConfig = {
        ...$dbosConfig,
        telemetry: { ...telemetry, logs: { ...telemetry?.logs, forceConsole: true } },
      };

      await setUpDBOSTestDb(dbosConfig);
      const dbosExec = new DBOSExecutor(dbosConfig);

      await dbosExec.init();
      expect(mockConsoleLog).toHaveBeenCalled();
      await dbosExec.destroy();
    } finally {
      mockConsoleLog.mockRestore();
    }
  });
});
