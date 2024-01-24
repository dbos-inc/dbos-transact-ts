exports.up = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {
            // Add CREATED_AT column with default value as current epoch time in milliseconds
            table.bigInteger('created_at')
                .notNullable()
                .defaultTo(knex.raw('(EXTRACT(EPOCH FROM now())*1000)::bigint'));

            // Add UPDATED_AT column with default value as current epoch time in milliseconds
            table.bigInteger('updated_at')
                .notNullable()
                .defaultTo(knex.raw('(EXTRACT(EPOCH FROM now())*1000)::bigint'));
        });
};

exports.down = function(knex) {
    return knex.schema.withSchema('dbos')
        .table('workflow_status', function(table) {
            table.dropColumn('created_at');
            table.dropColumn('updated_at');
        });
};
