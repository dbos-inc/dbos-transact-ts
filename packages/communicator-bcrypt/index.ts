
import {Communicator, CommunicatorContext} from '@dbos-inc/dbos-sdk';

class BcryptCommunicator
{
    @Communicator()
    static bcrypt(_ctx: CommunicatorContext) : Promise<number> {
        // TODO
        return Promise.resolve(1);
    }
}

export
{
    BcryptCommunicator
}