const { KnexDataSource } = require('@dbos-inc/knex-datasource');

exports.up = async function (knex) {
  await KnexDataSource.initializeDBOSSchema(knex);
};

exports.down = async function (knex) {
  await KnexDataSource.uninitializeDBOSSchema(knex);
};
