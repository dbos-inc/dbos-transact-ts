exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {

            table.bigInteger('workflow_retries')
                .defaultTo(0);
        });
};

exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {
            table.dropColumn('workflow_retries');
        });
};
