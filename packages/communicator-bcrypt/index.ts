
import {Step, StepContext} from '@dbos-inc/dbos-sdk';

import bcryptjs from 'bcryptjs';


class BcryptCommunicator
{
    @Step()
    static async bcryptGenSalt(_ctx: StepContext, saltRounds: number = 10) : Promise<string>
    {
        return await bcryptjs.genSalt(saltRounds);
    }

    @Step()
    static async bcryptHash(_ctx: StepContext, txt: string, saltRounds: number = 10) : Promise<string> {
        return await bcryptjs.hash(txt, saltRounds);
    }

    static async bcryptCompare(txt: string, hashedTxt: string): Promise<boolean> {
        const isMatch = bcryptjs.compare(txt, hashedTxt);
        return isMatch;
    }
}

export
{
    BcryptCommunicator
}
