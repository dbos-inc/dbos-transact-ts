import { MigrationInterface, QueryRunner } from "typeorm";

export class DBOSHello1714934318136 implements MigrationInterface {
    name = 'DBOSHello1714934318136';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`CREATE TABLE "dbos_hello" ("greeting_id" SERIAL NOT NULL, "greeting" character varying NOT NULL, CONSTRAINT "PK_12681863ff5f1fd5ea35f185b51" PRIMARY KEY ("greeting_id"))`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE "dbos_hello"`);
    }

}
