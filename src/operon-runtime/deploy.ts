import axios from "axios";
import { execSync } from "child_process";

export async function deploy(appName: string, host: string) {

    const response = await axios.post(
        `http://${host}:8080/application/register`,
        {
            name: appName,
        },
        {
            headers: {
                'Content-Type': 'application/json',
            },
        },
    );
    const uuid = response.data as string;
    execSync(`mkdir -p operon_deploy`);
    execSync(`rm -rf operon_deploy/*`);
    execSync(`zip -ry operon_deploy/${appName}.zip ./*`)
}