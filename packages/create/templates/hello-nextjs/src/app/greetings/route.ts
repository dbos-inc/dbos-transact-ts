import { NextResponse } from "next/server";
import { dbosWorkflow } from "@/actions/dbosWorkflow";
import { DBOS } from "@dbos-inc/dbos-sdk";

export async function POST(request: Request) {
  const body = await request.json();
  const { userName } = body;
  DBOS.logger.info(`Received request with name: ${userName}`);
  const response = await dbosWorkflow(userName);
  return NextResponse.json(response);
}
