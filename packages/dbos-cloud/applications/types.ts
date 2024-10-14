export type Application = {
  Name: string;
  ID: string;
  PostgresInstanceName: string;
  ApplicationDatabaseName: string;
  Status: string;
  Version: string;
  AppURL: string;
  ExecutorsMemoryMib: number;
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
  ApplicationName: string;
  Version: string;
  CreationTime: string;
};

export function prettyPrintApplicationVersion(version: ApplicationVersion) {
  console.log(`Application Name: ${version.ApplicationName}`);
  console.log(`Version: ${version.Version}`);
  console.log(`Creation Timestamp: ${version.CreationTime}`);
}

// Either types.ts should be in the parent folder, or UserDBInstance should be in databases/types.ts
export interface UserDBInstance {
  readonly PostgresInstanceName: string;
  readonly Status: string;
  readonly HostName: string;
  readonly Port: number;
  readonly DatabaseUsername: string;
}
