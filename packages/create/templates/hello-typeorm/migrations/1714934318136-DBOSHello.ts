import { MigrationInterface, QueryRunner } from "typeorm";

// This migration file was automatically generated from the project entity files (entities/*.ts).
// This was done by running on an empty database the command: `npx typeorm migration:generate -d dist/datasource.js migrations/DBOSHello`
// This file creates the schema defined in the entity files.
// For more information on schema management with TypeORM and DBOS, see our docs: https://docs.dbos.dev/tutorials/using-typeorm

export class DBOSHello1714934318136 implements MigrationInterface {
    name = 'DBOSHello1714934318136';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "dbos_hello" ("greeting_id" SERIAL NOT NULL, "greeting" character varying NOT NULL, CONSTRAINT "PK_12681863ff5f1fd5ea35f185b51" PRIMARY KEY ("greeting_id"))`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "dbos_hello"`);
    }

}
