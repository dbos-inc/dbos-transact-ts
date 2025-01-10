import { NextResponse } from "next/server";
import { DBOS } from "@dbos-inc/dbos-sdk";

export async function GET(_request: Request, { params }: { params: Promise<{ slug: string }> })
{
    const taskId = (await params).slug;
    DBOS.logger.info(`Received request to check on taskId: ${taskId}`);

    let step = await DBOS.getEvent(taskId, "steps_event");

    DBOS.logger.info(`For taskId: ${taskId} we are done with ${step} steps`);  

    return NextResponse.json({ "stepsCompleted": step});
}