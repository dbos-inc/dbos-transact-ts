/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
    .alterTable('workflow_status', function(table) {
        table.text('application_version');
        table.text('application_id');
    })
};

/**
 * @param { import("knex").Knex } knex
 * @returns { Promise<void> }
 */
exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
    .alterTable('workflow_status', function(table) {
        table.dropColumn('application_version');
        table.dropColumn('application_id');
    })
};
