import axios from "axios";

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
    console.log(uuid);
}