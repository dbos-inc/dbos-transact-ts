import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  return knex.schema.createTable('operon_hello', table => {
    table.increments('greeting_id').primary();
    table.text('greeting');
  });
}

export async function down(knex: Knex): Promise<void> {
  return knex.schema.dropTable('operon_hello');
}

