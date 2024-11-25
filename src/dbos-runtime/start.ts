import { spawn } from "child_process";
import { GlobalLogger } from "../telemetry/logs";

// Run the "start" command provided by users in their dbos-config.yaml
export function runStartCommand(command: string, logger: GlobalLogger) {
  // Split the command into the executable and its arguments
  const [executable, ...args] = command.split(" ");

  // Spawn a child process
  const child = spawn(executable, args, {
    stdio: "inherit", // Forward stdin, stdout, stderr
    shell: true, // Use the shell to interpret the command
  });

  // Handle parent signals and forward to child
  const handleSignal = (signal: NodeJS.Signals) => {
    logger.info(`Received ${signal}, forwarding to child process...`);
    if (child.pid) {
      process.kill(child.pid, signal);
    }
  };

  process.on("SIGTERM", () => handleSignal("SIGTERM"));
  process.on("SIGINT", () => handleSignal("SIGINT"));

  // Cleanup when child exits
  child.on("exit", (code, signal) => {
    logger.info(`Child process exited with code ${code} or signal ${signal}`);
    process.exit(code ?? (signal ? 1 : 0));
  });

  // Must be caught by the caller
  child.on("error", (error) => {
    logger.error(`Failed to start child process: ${error.message}`);
    process.exit(1);
  });
}
