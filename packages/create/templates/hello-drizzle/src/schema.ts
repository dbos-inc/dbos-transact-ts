import { integer } from "drizzle-orm/pg-core";
import { pgTable, text } from "drizzle-orm/pg-core";

export const DBOSHello = pgTable('dbos_hello', {
  name: text('name').primaryKey(),
  greet_count: integer('greet_count').default(0)
});
