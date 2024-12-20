import { NextResponse } from "next/server";
import { DBOS } from "@dbos-inc/dbos-sdk";

export async function GET(request: Request, { params }: { params: Promise<{ slug: string }> }) {

    const taskId = (await params).slug;
    DBOS.logger.info(`Received request with taskId: ${taskId}`);

    let step = DBOS.getEvent(taskId, "steps_event");

    return NextResponse.json(step);

}