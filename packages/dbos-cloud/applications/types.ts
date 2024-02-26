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