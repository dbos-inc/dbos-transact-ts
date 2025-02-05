import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
  await knex.schema.createTable('user_files', (table) => {
    table.uuid('file_id').primary();
    table.uuid('user_id'); //.index().references("cusers.user_id");
    table.string('file_status', 16);
    table.string('file_type', 16);
    table.bigint('file_time');
    table.string('file_name', 128);
  });
}

export async function down(knex: Knex): Promise<void> {
  await knex.schema.dropTable('user_files');
}
