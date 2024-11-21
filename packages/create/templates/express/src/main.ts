import { app } from './operations';
import { DBOS } from '@dbos-inc/dbos-sdk';

async function main() {
  app.listen(9000);

  await DBOS.launch({expressApp: app});
}

main().then(()=>{}).catch((e) => console.log(e));