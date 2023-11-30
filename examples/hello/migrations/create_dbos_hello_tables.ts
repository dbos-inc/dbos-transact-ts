import { Knex } from "knex";

export async function up(knex: Knex): Promise<void> {
  return knex.schema.createTable('dbos_hello', table => {
    table.text('name').primary();
    table.integer('greet_count').defaultTo(0);
  });
}

export async function down(knex: Knex): Promise<void> {
  return knex.schema.dropTable('dbos_hello');
}
