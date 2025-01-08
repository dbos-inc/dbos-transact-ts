
export async function GET(request: Request) {

    console.log("Received request Crashing the app");

    process.exit(1);

}