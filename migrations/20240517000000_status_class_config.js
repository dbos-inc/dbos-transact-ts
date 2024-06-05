exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {
            table.string('class_name')
                .defaultTo(null);

            table.string('config_name')
                .defaultTo(null);
        });
};

exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {
            table.dropColumn('class_name');
            table.dropColumn('config_name');
        });
};
