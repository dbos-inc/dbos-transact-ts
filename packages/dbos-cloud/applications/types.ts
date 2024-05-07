export type Application = {
  Name: string;
  ID: string;
  PostgresInstanceName: string;
  ApplicationDatabaseName: string;
  Status: string;
  Version: string;
  AppURL: string;
};

export function prettyPrintApplication(app: Application) {
  console.log(`Application Name: ${app.Name}`);
  console.log(`ID: ${app.ID}`);
  console.log(`Postgres Instance Name: ${app.PostgresInstanceName}`);
  console.log(`Application Database Name: ${app.ApplicationDatabaseName}`);
  console.log(`Status: ${app.Status}`);
  console.log(`Version: ${app.Version}`);
  console.log(`App URL: ${app.AppURL}`);
}

export type ApplicationVersion = {
  application_name: string;
  version: string;
  creation_time: string;
}

export function prettyPrintApplicationVersion(version: ApplicationVersion) {
  console.log(`Application Name: ${version.application_name}`);
  console.log(`Version: ${version.version}`);
  console.log(`Creation Timestamp: ${version.creation_time}`);
}