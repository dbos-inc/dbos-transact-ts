exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {

            table.bigInteger('recovery_attempts')
                .defaultTo(0);
        });
};

exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {
            table.dropColumn('recovery_attempts');
        });
};
