
import {ArgOptional, Communicator, CommunicatorContext, DBOS} from '@dbos-inc/dbos-sdk';

import bcryptjs from 'bcryptjs';


class BcryptCommunicator
{
    @Communicator()
    static async bcryptGenSalt(_ctx: CommunicatorContext, saltRounds: number = 10) : Promise<string>
    {
        return await bcryptjs.genSalt(saltRounds);
    }

    @Communicator()
    static async bcryptHash(_ctx: CommunicatorContext, txt: string, saltRounds: number = 10) : Promise<string> {
        return await bcryptjs.hash(txt, saltRounds);
    }

    static async bcryptCompare(txt: string, hashedTxt: string): Promise<boolean> {
        const isMatch = bcryptjs.compare(txt, hashedTxt);
        return isMatch;
    }
}

class BcryptStepV2
{
    @DBOS.step()
    static async bcryptGenSalt(@ArgOptional saltRounds?: number) : Promise<string>
    {
        if (!saltRounds) saltRounds = 10;
        return await bcryptjs.genSalt(saltRounds);
    }

    @DBOS.step()
    static async bcryptHash(txt: string, @ArgOptional saltRounds?: number) : Promise<string> {
        if (!saltRounds) saltRounds = 10;
        return await bcryptjs.hash(txt, saltRounds);
    }

    static async bcryptCompare(txt: string, hashedTxt: string): Promise<boolean> {
        const isMatch = bcryptjs.compare(txt, hashedTxt);
        return isMatch;
    }
}

export
{
    BcryptCommunicator,
    BcryptCommunicator as BcryptStepV1,
    BcryptStepV2 as BcryptStep,
}
