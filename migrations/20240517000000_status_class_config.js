exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {
            // Add CREATED_AT column with default value as current epoch time in milliseconds
            table.string('class_name')
                .defaultTo(null);

            // Add UPDATED_AT column with default value as current epoch time in milliseconds
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
