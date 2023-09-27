import { Operon, } from 'operon';
import { Hello } from './userFunctions';
import Jabber from 'jabber';
import { Client, PoolConfig } from 'pg';

async function ensureHelloDatabase(config: PoolConfig) {
  const client = new Client({ ...config, database: "postgres" });
  await client.connect();
  const { rowCount } = await client.query("SELECT FROM pg_database WHERE datname = $1", [config.database]);
  if (rowCount === 0) {
    await client.query(`CREATE DATABASE ${config.database}`);
  }
  await client.end();
}

async function main(name: string) {

  const operon = new Operon();
  await ensureHelloDatabase(operon.config.poolConfig);

  operon.useNodePostgres();
  await operon.userDatabase.query("CREATE TABLE IF NOT EXISTS OperonHello (greeting_id SERIAL PRIMARY KEY, greeting TEXT);");
  await operon.init(Hello);

  const result = await operon.workflow(Hello.helloWorkflow, {}, name).getResult();
  console.log(`Result: ${result}`);
  await operon.destroy();
}

const jabber = new Jabber();
const name = jabber.createFullName(false);
console.log(`Name:   ${name}`);
main(name).catch(reason => {
  console.log(reason);
});