const postgresLogBackendSchema = `
  CREATE TABLE IF NOT EXISTS log_signal (
      workflow_instance_id VARCHAR(255) NOT NULL,
      function_id VARCHAR(255) NOT NULL,
      log_signal_raw VARCHAR(255) NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (workflow_instance_id, function_id)
  );
`;

export default postgresLogBackendSchema;
