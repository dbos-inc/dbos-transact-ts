import { app } from './operations';
import { DBOS } from '@dbos-inc/dbos-sdk';

async function main() {
  const PORT = 9000;
  const ENV = process.env.NODE_ENV || 'development';

  await DBOS.launch({expressApp: app});

  app.listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`);
    console.log(`🌟 Environment: ${ENV}`);
  });
}

main().then(()=>{}).catch((e) => console.log(e));