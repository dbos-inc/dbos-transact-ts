import { generateOperonTestConfig, setupOperonTestDb } from "../helpers";
import { OperonConfig } from "../../src";
import { ProvenanceDaemon } from "../../src/provenance/provenance_daemon";

describe("operon-provenance", () => {

  let config: OperonConfig;

  beforeAll(async () => {
    config = generateOperonTestConfig();
    await setupOperonTestDb(config);
  });


  test("basic-provenance", async () => {
    const provDaemon = new ProvenanceDaemon(config.poolConfig, "jest_test_slot");
    await provDaemon.start();
    await provDaemon.stop();

    // The new daemon tries to create the same slot, which will be handled correctly.
    const provDaemon2 = new ProvenanceDaemon(
      config.poolConfig,
      "jest_test_slot"
    );
    await provDaemon2.start();
    await provDaemon2.stop();
  });
});
