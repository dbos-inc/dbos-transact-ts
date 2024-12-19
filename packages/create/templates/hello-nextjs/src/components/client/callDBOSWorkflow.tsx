"use client";

import { useState } from "react";

export default function CallDBOSWorkflow( { wfResult }: { wfResult: string }) {
    const [greetingsWfResult, setGreetingsWfResult] = useState<string>(wfResult);
    const [loading, setLoading] = useState<boolean>(false);

    const greetUser = async () => {
        setLoading(true);
        try {
            const response = await fetch("/greetings", { method: "POST", body: JSON.stringify({ userName: "dbosuser" }) });
            const data = await response.json();
            setGreetingsWfResult(data);
        } catch (error) {
            console.error("Error calling greetings workflow", error);
            setGreetingsWfResult("Error calling greetings workflow: " + error);
        } finally {
            setLoading(false);
        }
    }

    return(
        <div>
            <button onClick={greetUser} disabled={loading} className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600">
                {loading ? "" : "Run DBOS Workflow"}
            </button>
            <p>{greetingsWfResult}</p>
        </div>
  );
}
