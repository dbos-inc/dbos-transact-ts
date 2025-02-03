exports.up = function (knex) {
  return knex.schema.alterTable("dbos.workflow_status", (table) => {
    table.bigInteger("recovery_attempts").defaultTo(1).alter();
  });
};

exports.down = function (knex) {
  return knex.schema.alterTable("dbos.workflow_status", (table) => {
    table.bigInteger("recovery_attempts").defaultTo(0).alter();
  });
};

