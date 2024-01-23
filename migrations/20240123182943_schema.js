exports.up = function(knex) {
    return knex.raw('CREATE SCHEMA IF NOT EXISTS dbos');
  };
  
  exports.down = function(knex) {
    return knex.raw('DROP SCHEMA IF EXISTS dbos');
  };
  