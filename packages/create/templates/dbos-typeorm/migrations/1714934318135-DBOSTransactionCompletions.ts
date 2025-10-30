import { MigrationInterface, QueryRunner } from 'typeorm';

import { createTransactionCompletionSchemaPG, createTransactionCompletionTablePG } from '@dbos-inc/dbos-sdk/datasource';

export class DBOSTransactionCompletions1714934318135 implements MigrationInterface {
  name = 'DBOSTransactionCompletions1714934318135';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(createTransactionCompletionSchemaPG());
    await queryRunner.query(createTransactionCompletionTablePG());
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query('DROP SCHEMA IF EXISTS dbos CASCADE');
  }
}
