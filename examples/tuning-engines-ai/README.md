# Tuning Engines governed AI workflow

This example shows the shape of a DBOS workflow that calls Tuning Engines from
a durable step. DBOS owns workflow recovery, queues, and database-backed state.
Tuning Engines owns model routing, policy checks, approval decisions, usage
attribution, and trace correlation.

## Environment

```bash
export TE_INFERENCE_KEY=sk-te-your-inference-key
export TE_MODEL=auto
```

## Workflow

```ts
import { DBOS } from "@dbos-inc/dbos-sdk";

type Input = {
  prompt: string;
  run_id?: string;
};

function newId(prefix: string): string {
  return `${prefix}_${crypto.randomUUID().replaceAll("-", "")}`;
}

async function governedModelStep(input: Required<Input>) {
  const request_id = newId("req");
  const response = await fetch("https://api.tuningengines.com/v1/chat/completions", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.TE_INFERENCE_KEY}`,
      "Content-Type": "application/json",
      "X-TE-Run-ID": input.run_id,
      "X-TE-Request-ID": request_id,
    },
    body: JSON.stringify({
      model: process.env.TE_MODEL || "auto",
      messages: [{ role: "user", content: input.prompt }],
      metadata: {
        run_id: input.run_id,
        request_id,
        runtime: "dbos",
        event_type: "model.call",
      },
    }),
  });

  if (!response.ok) {
    throw new Error(`Tuning Engines request failed: ${response.status} ${await response.text()}`);
  }

  return response.json();
}

async function governedAiWorkflow(input: Input) {
  const run_id = input.run_id || newId("dbos");
  return DBOS.runStep(
    () => governedModelStep({ ...input, run_id }),
    { name: "tuning-engines-model-call" },
  );
}

export const workflow = DBOS.registerWorkflow(governedAiWorkflow, {
  name: "tuningEnginesGovernedAi",
});
```

Use the same `run_id` on any trace or state-reference calls you emit after the
model step so gateway events can be correlated with DBOS workflow history.
