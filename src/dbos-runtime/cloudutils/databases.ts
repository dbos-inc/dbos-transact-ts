import axios, { AxiosError } from "axios";
import { isCloudAPIErrorResponse, handleAPIErrors, getCloudCredentials, getLogger, sleepms, DBOSCloudCredentials } from "./cloudutils";
import { Logger } from "winston";
import { select } from "@inquirer/prompts";

export interface UserDBInstance {
    readonly PostgresInstanceName: string;
    readonly Status: string;
    readonly HostName: string;
    readonly Port: number;
    readonly DatabaseUsername: string;
    readonly IsLinked: boolean;
    readonly SupabaseReference: string | null;
}


function isValidPassword(logger: Logger, password: string): boolean {
    if (password.length < 8 || password.length > 128) {
        logger.error("Invalid database password. Passwords must be between 8 and 128 characters long");
        return false;
    }
    if (password.includes("/") || password.includes('"') || password.includes("@") || password.includes(" ") || password.includes("'")) {
        logger.error("Password contains invalid character. Passwords can contain any ASCII character except @, /, \\, \", ', and spaces");
        return false;
    }
    return true;
}

export async function createUserDb(host: string, dbName: string, appDBUsername: string, appDBPassword: string, sync: boolean, userCredentials?: DBOSCloudCredentials) {
    const logger = getLogger();
    if (!userCredentials) {
        userCredentials = await getCloudCredentials(host, logger);
    }
    const bearerToken = "Bearer " + userCredentials.token;

    if (!isValidPassword(logger, appDBPassword)) {
        return 1;
    }

    try {
        await axios.post(
            `https://${host}/v1alpha1/${userCredentials.organization}/databases/userdb`,
            { Name: dbName, AdminName: appDBUsername, AdminPassword: appDBPassword },
            {
                headers: {
                    "Content-Type": "application/json",
                    Authorization: bearerToken,
                },
            }
        );

        logger.info(`Successfully started provisioning database: ${dbName}`);

        if (sync) {
            let status = "";
            while (status !== "available" && status !== "backing-up") {
                if (status === "") {
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

export async function getUserDBInfo(host: string, dbName: string, userCredentials?: DBOSCloudCredentials): Promise<UserDBInstance> {
    const logger = getLogger();
    if (!userCredentials) {
        userCredentials = await getCloudCredentials(host, logger);
    }
    const bearerToken = "Bearer " + userCredentials.token;

    const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/databases/userdb/info/${dbName}`, {
        headers: {
            "Content-Type": "application/json",
            Authorization: bearerToken,
        },
    });

    return res.data as UserDBInstance;
}

export async function chooseAppDBServer(logger: Logger, host: string, userCredentials: DBOSCloudCredentials): Promise<string> {
    // List existing database instances.
    let userDBs: UserDBInstance[] = [];
    const bearerToken = "Bearer " + userCredentials.token;
    try {
        const res = await axios.get(`https://${host}/v1alpha1/${userCredentials.organization}/databases`, {
            headers: {
                "Content-Type": "application/json",
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
        return "";
    }

    let userDBName = "";

    if (userDBs.length === 0) {
        // If not, prompt the user to provision one.
        logger.info("Provisioning a cloud Postgres database server");
        userDBName = `${userCredentials.userName}-db-server`;
        // Use a default user name and auto generated password.
        const appDBUserName = "dbos_user";
        const appDBPassword = Buffer.from(Math.random().toString()).toString("base64");
        const res = await createUserDb(host, userDBName, appDBUserName, appDBPassword, true);
        if (res !== 0) {
            return "";
        }
    } else if (userDBs.length > 1) {
        // If there is more than one database instances, prompt the user to select one.
        userDBName = await select({
            message: "Choose a database instance for this app:",
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
