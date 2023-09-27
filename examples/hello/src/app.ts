import { Operon, } from 'operon';
import { Hello } from './userFunctions';

import Jabber from 'jabber';

async function main(name: string) {
    const operon = new Operon();
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