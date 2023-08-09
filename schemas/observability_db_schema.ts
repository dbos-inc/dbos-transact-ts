const postgresLogBackendSchema = `
  CREATE TABLE IF NOT EXISTS log_signal (
      workflow_uuid TEXT NOT NULL,
      function_id INT NOT NULL,
      log_signal_raw VARCHAR(255) NOT NULL,
      created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()))::bigint,
      updated_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now()))::bigint,
      PRIMARY KEY (workflow_uuid, function_id)
  );
`;

export default postgresLogBackendSchema;
