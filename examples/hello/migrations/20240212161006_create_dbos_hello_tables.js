const { Knex } = require("knex");

exports.up = async function(knex) {
  return knex.schema.createTable('dbos_hello', table => {
    table.text('name');
    table.text('greeting_note_content');
    table.text('greeting_note_uuid');
    table.integer('greet_count').defaultTo(0);
  });
};

exports.down = async function(knex) {
  return knex.schema.dropTable('dbos_hello');
};
