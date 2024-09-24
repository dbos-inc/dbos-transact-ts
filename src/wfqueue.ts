import { DBOSInitializationError } from "./error";

export const wfQueuesByName: Map<string, WorkflowQueue> = new Map();

export class WorkflowQueue {
    constructor(readonly name: string, readonly concurrency?: number) {
        if (wfQueuesByName.has(name)) {
            throw new DBOSInitializationError(`Workflow Queue '${name}' defined multiple times`);
        }
        wfQueuesByName.set(name, this);
    }
}

/*
+def queue_thread(stop_event: threading.Event, dbos: "DBOS") -> None:
+    while not stop_event.is_set():
+        time.sleep(1)
+        for queue_name, queue in dbos._registry.queue_info_map.items():
+            wf_ids = dbos._sys_db.start_queued_workflows(queue_name, queue.concurrency)
+            for id in wf_ids:
+                _execute_workflow_id(dbos, id)
*/