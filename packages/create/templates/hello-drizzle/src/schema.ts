import { serial } from "drizzle-orm/pg-core";
import { pgTable, text } from "drizzle-orm/pg-core";

export const dbosHello = pgTable('dbos_hello', {
  greet_count: serial('greet_count').primaryKey(),
  greeting: text('greeting'),
});
