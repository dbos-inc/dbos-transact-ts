import { LoggerConfig } from './logs';
import { OTLPExporterConfig } from './exporters';

export interface TelemetryConfig {
  logs?: LoggerConfig;
  OTLPExporter?: OTLPExporterConfig;
}
