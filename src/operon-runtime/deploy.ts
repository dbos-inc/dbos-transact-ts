import axios from "axios";
import { execSync } from "child_process";
import fs from 'fs';
import FormData from 'form-data';

export async function deploy(appName: string, host: string) {

    try {
        const register = await axios.post(
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
        const uuid = register.data as string;
        execSync(`mkdir -p operon_deploy`);
        execSync(`zip -ry operon_deploy/${uuid}.zip ./* -x "operon_deploy/*"`)

        const formData = new FormData();
        formData.append('app_archive', fs.createReadStream(`operon_deploy/${uuid}.zip`));

        await axios.post(`http://localhost:8080/application/${uuid}`, formData, {
            headers: {
                ...formData.getHeaders(),
            },
        });
        console.log(`Successfully deployed: ${appName}`);
        console.log(`${appName} ID: ${uuid}`)
    } catch (e) {
        console.log(`Deploying ${appName} failed`);
        throw e;
    }
}