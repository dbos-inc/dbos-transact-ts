import { NextResponse } from "next/server";
import { DBOS } from "@dbos-inc/dbos-sdk";
import { dbosBackgroundTask } from "@/actions/dbosWorkflow";    

export async function GET(request: Request, { params }: { params: Promise<{ slug: string }> }) {

    const taskId = (await params).slug;

    DBOS.logger.info(`Received request to start background task taskId: ${taskId}`);

    DBOS.logger.info(`Started background task taskId: ${taskId}`);

    await dbosBackgroundTask(taskId)

    return NextResponse.json({ message: "Background task started" });

}

