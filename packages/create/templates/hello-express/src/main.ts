import { app } from './operations';
import { DBOS } from '@dbos-inc/dbos-sdk';

async function main() {
  await DBOS.launch({expressApp: app});

  const PORT = DBOS.runtimeConfig?.port ?? 3000;
  const ENV = process.env.NODE_ENV || 'development';

  app.listen(PORT, () => {
    console.log(`🚀 Server is running on http://localhost:${PORT}`);
    console.log(`🌟 Environment: ${ENV}`);
  });
}

main().catch(console.log);