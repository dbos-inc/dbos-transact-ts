import { LoggerConfig } from "./logs";
import { TracerConfig } from "./traces";

export interface TelemetryConfig {
  logs?: LoggerConfig;
  traces?: TracerConfig;
}
