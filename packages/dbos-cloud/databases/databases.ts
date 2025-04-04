import axios, { AxiosError } from 'axios';
import {
  isCloudAPIErrorResponse,
  handleAPIErrors,
  getCloudCredentials,
  getLogger,
  sleepms,
  dbosConfigFilePath,
  DBOSCloudCredentials,
} from '../cloudutils.js';
import { Logger } from 'winston';
import { copyFileSync, existsSync } from 'fs';
import { UserDBInstance } from '../applications/types.js';
import { input, select } from '@inquirer/prompts';
import promptSync from 'prompt-sync';

function isValidPassword(logger: Logger, password: string): boolean {
  if (password.length < 8 || password.length > 128) {
    logger.error('Invalid database password. Passwords must be between 8 and 128 characters long');
    return false;
  }
  if (
    password.includes('/') ||
    password.includes('"') ||
    password.includes('@') ||
    password.includes(' ') ||
    password.includes("'")
  ) {
    logger.error(
      'Password contains invalid character. Passwords can contain any ASCII character except @, /, \\, ", \', and spaces',
    );
    return false;
  }
  return true;
}

export async function createUserDb(
  host: string,
  dbName: string,
  appDBUsername: string,
  appDBPassword: string,
  sync: boolean,
  userCredentials?: DBOSCloudCredentials,
) {
  const logger = getLogger();
  if (!userCredentials) {
    userCredentials = await getCloudCredentials(host, logger);
  }
  const bearerToken = 'Bearer ' + userCredentials.token;

  if (!isValidPassword(logger, appDBPassword)) {
    return 1;
  }

  try {
    await axios.post(
      `https://${host}/v1alpha1/${userCredentials.organization}/databases/userdb`,
      { Name: dbName, AdminName: appDBUsername, AdminPassword: appDBPassword },
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );

    logger.info(`Successfully started provisioning database: ${dbName}`);

    if (sync) {
      let status = '';
      while (status !== 'available' && status !== 'backing-up') {
        if (status === '') {
          await sleepms(5000); // First time sleep 5 sec
        } else {
          await sleepms(30000); // Otherwise, sleep 30 sec
        }
        const userDBInfo = await getUserDBInfo(host, dbName, userCredentials);
        logger.info(userDBInfo);
        status = userDBInfo.Status;
      }
    }
    logger.info(`Database successfully provisioned!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to create database ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function linkUserDB(
  host: string,
  dbName: string,
  hostName: string,
  port: number,
  dbPassword: string,
  enableTimetravel: boolean,
  supabaseReference: string | undefined,
) {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  if (!isValidPassword(logger, dbPassword)) {
    return 1;
  }

  let data = {};
  if (supabaseReference === undefined) {
    data = { Name: dbName, HostName: hostName, Port: port, Password: dbPassword, captureProvenance: enableTimetravel };
  } else {
    data = {
      Name: dbName,
      HostName: hostName,
      Port: port,
      Password: dbPassword,
      captureProvenance: enableTimetravel,
      supabaseReference: supabaseReference,
    };
  }

  logger.info(
    `Linking Postgres instance ${dbName} to DBOS Cloud. Hostname: ${hostName} Port: ${port} Time travel: ${enableTimetravel} Supabase Reference: ${supabaseReference}`,
  );
  try {
    await axios.post(`https://${host}/v1alpha1/${userCredentials.organization}/databases/byod`, data, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    });

    logger.info(`Database successfully linked!`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to link database ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function deleteUserDb(host: string, dbName: string) {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  try {
    await axios.delete(`https://${host}/v1alpha1/${userCredentials.organization}/databases/userdb/${dbName}`, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    });
    logger.info(`Database deleted: ${dbName}`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to delete database ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function unlinkUserDB(host: string, dbName: string) {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  try {
    await axios.delete(`https://${host}/v1alpha1/${userCredentials.organization}/databases/byod/${dbName}`, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    });
    logger.info(`Database unlinked: ${dbName}`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to unlink database ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function getUserDb(host: string, dbName: string, json: boolean) {
  const logger = getLogger();

  try {
    const userDBInfo = await getUserDBInfo(host, dbName);
    if (json) {
      console.log(JSON.stringify(userDBInfo));
    } else {
      console.log(`Postgres Instance Name: ${userDBInfo.PostgresInstanceName}`);
      console.log(`Status: ${userDBInfo.Status}`);
      console.log(`Host Name: ${userDBInfo.HostName}`);
      console.log(`Port: ${userDBInfo.Port}`);
      console.log(`Database Username: ${userDBInfo.DatabaseUsername}`);
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve database record ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function listUserDB(host: string, json: boolean) {
  const logger = getLogger();

  try {
    const userCredentials = await getCloudCredentials(host, logger);
    const bearerToken = 'Bearer ' + userCredentials.token;

    const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/databases`, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    });

    const userDBs = res.data as UserDBInstance[];
    if (json) {
      console.log(JSON.stringify(userDBs));
    } else {
      if (userDBs.length === 0) {
        logger.info('No database instances found');
      }
      userDBs.forEach((userDBInfo) => {
        console.log(`Postgres Instance Name: ${userDBInfo.PostgresInstanceName}`);
        console.log(`Status: ${userDBInfo.Status}`);
        console.log(`Host Name: ${userDBInfo.HostName}`);
        console.log(`Port: ${userDBInfo.Port}`);
        console.log(`Database Username: ${userDBInfo.DatabaseUsername}`);
      });
    }
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve info`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

async function getUserDBInfo(
  host: string,
  dbName: string,
  userCredentials?: DBOSCloudCredentials,
): Promise<UserDBInstance> {
  const logger = getLogger();
  if (!userCredentials) {
    userCredentials = await getCloudCredentials(host, logger);
  }
  const bearerToken = 'Bearer ' + userCredentials.token;

  const res = await axios.get(
    `https://${host}/v1alpha1/${userCredentials.organization}/databases/userdb/info/${dbName}`,
    {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    },
  );

  return res.data as UserDBInstance;
}

export async function resetDBCredentials(host: string, dbName: string | undefined, appDBPassword: string | undefined) {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);

  dbName = await chooseAppDBServer(logger, host, userCredentials, dbName);
  if (dbName === '') {
    return 1;
  }

  const bearerToken = 'Bearer ' + userCredentials.token;

  try {
    const userDBInfo = await getUserDBInfo(host, dbName, userCredentials);

    if (userDBInfo.IsLinked) {
      if (userDBInfo.SupabaseReference !== null) {
        logger.error(
          'The DBOS CLI cannot reset the password of your Supabase database. Please reset it from your Supabase dashboard.',
        );
      } else {
        logger.error('Error: You cannot reset the password of a linked database from the DBOS CLI.');
      }
      return 1;
    }

    const prompt = promptSync({ sigint: true });
    if (!appDBPassword) {
      appDBPassword = prompt('Database Password (must contain at least 8 characters): ', { echo: '*' });
    }
    if (!isValidPassword(logger, appDBPassword)) {
      return 1;
    }

    await axios.post(
      `https://${host}/v1alpha1/${userCredentials.organization}/databases/userdb/${dbName}/credentials`,
      { Password: appDBPassword },
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );
    logger.info(`Successfully reset user password for database: ${dbName}`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to reset user password for database ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function restoreUserDB(
  host: string,
  dbName: string,
  targetName: string,
  restoreTime: string,
  sync: boolean,
) {
  const logger = getLogger();
  const userCredentials = await getCloudCredentials(host, logger);
  const bearerToken = 'Bearer ' + userCredentials.token;

  try {
    await axios.post(
      `https://${host}/v1alpha1/${userCredentials.organization}/databases/userdb/${dbName}/restore`,
      { RestoreName: targetName, RestoreTimestamp: restoreTime },
      {
        headers: {
          'Content-Type': 'application/json',
          Authorization: bearerToken,
        },
      },
    );
    logger.info(
      `Successfully started restoring database: ${dbName}! New database name: ${targetName}, restore time: ${restoreTime}`,
    );

    if (sync) {
      let status = '';
      while (status !== 'available' && status !== 'backing-up') {
        if (status === '') {
          await sleepms(5000); // First time sleep 5 sec
        } else {
          await sleepms(30000); // Otherwise, sleep 30 sec
        }
        const userDBInfo = await getUserDBInfo(host, targetName, userCredentials);
        logger.info(userDBInfo);
        status = userDBInfo.Status;
      }
    }
    logger.info(`Database successfully restored! New database name: ${targetName}, restore time: ${restoreTime}`);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to restore database ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function connect(
  host: string,
  dbName: string | undefined,
  password: string | undefined,
  showPassword: boolean,
) {
  const logger = getLogger();

  const userCredentials = await getCloudCredentials(host, logger);

  dbName = await chooseAppDBServer(logger, host, userCredentials, dbName);
  if (dbName === '') {
    return 1;
  }

  try {
    if (!existsSync(dbosConfigFilePath)) {
      logger.error(`Error: ${dbosConfigFilePath} not found`);
      return 1;
    }

    const backupConfigFilePath = `dbos-config.yaml.${Date.now()}.bak`;
    logger.info(`Backing up ${dbosConfigFilePath} to ${backupConfigFilePath}`);
    copyFileSync(dbosConfigFilePath, backupConfigFilePath);

    logger.info('Retrieving cloud database info...');
    const userDBInfo = await getUserDBInfo(host, dbName, userCredentials);
    const isSupabase = userDBInfo.SupabaseReference !== null;

    const prompt = promptSync({ sigint: true });
    if (!password) {
      if (isSupabase) {
        password = prompt('Enter Supabase Database Password: ', { echo: '*' });
      } else {
        password = prompt('Enter Database Password: ', { echo: '*' });
      }
    }

    const databaseUsername = isSupabase ? `postgres.${userDBInfo.SupabaseReference}` : userDBInfo.DatabaseUsername;

    console.log(`Postgres Instance Name: ${userDBInfo.PostgresInstanceName}`);
    console.log(`Host Name: ${userDBInfo.HostName}`);
    console.log(`Port: ${userDBInfo.Port}`);
    console.log(`Database Username: ${databaseUsername}`);
    console.log(`Status: ${userDBInfo.Status}`);

    const displayPassword = showPassword ? password : password.replace(/./g, '*');
    const dbString = `postgresql://${databaseUsername}:${displayPassword}@${userDBInfo.HostName}:${userDBInfo.Port}/${
      userDBInfo.PostgresInstanceName
    }`;
    console.log(dbString);
    return 0;
  } catch (e) {
    const errorLabel = `Failed to retrieve database record ${dbName}`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return 1;
  }
}

export async function chooseAppDBServer(
  logger: Logger,
  host: string,
  userCredentials: DBOSCloudCredentials,
  userDBName: string = '',
): Promise<string> {
  // List existing database instances.
  let userDBs: UserDBInstance[] = [];
  const bearerToken = 'Bearer ' + userCredentials.token;
  try {
    const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/databases`, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: bearerToken,
      },
    });
    userDBs = res.data as UserDBInstance[];
  } catch (e) {
    const errorLabel = `Failed to list databases`;
    const axiosError = e as AxiosError;
    if (isCloudAPIErrorResponse(axiosError.response?.data)) {
      handleAPIErrors(errorLabel, axiosError);
    } else {
      logger.error(`${errorLabel}: ${(e as Error).message}`);
    }
    return '';
  }

  if (userDBName) {
    // Check if the database instance exists or not.
    const dbExists = userDBs.some((db) => db.PostgresInstanceName === userDBName);
    if (dbExists) {
      return userDBName;
    }
  }

  if (userDBName || userDBs.length === 0) {
    // If not, prompt the user to provision one.
    if (!userDBName) {
      logger.info('No database found, provisioning a database instance (server)...');
      userDBName = await input({
        message: 'Database instance name?',
        default: `${userCredentials.userName}-db-server`,
      });
    } else {
      logger.info(`Database instance ${userDBName} not found, provisioning a new one...`);
    }

    // Use a default user name and auto generated password.
    const appDBUserName = 'dbos_user';
    const appDBPassword = Buffer.from(Math.random().toString()).toString('base64');
    const res = await createUserDb(host, userDBName, appDBUserName, appDBPassword, true);
    if (res !== 0) {
      return '';
    }
  } else if (userDBs.length > 1) {
    // If there is more than one database instances, prompt the user to select one.
    userDBName = await select({
      message: 'Choose a database instance for this app:',
      choices: userDBs.map((db) => ({
        name: db.PostgresInstanceName,
        value: db.PostgresInstanceName,
      })),
    });
  } else {
    // Use the only available database server.
    userDBName = userDBs[0].PostgresInstanceName;
    logger.info(`Using database instance: ${userDBName}`);
  }
  return userDBName;
}
