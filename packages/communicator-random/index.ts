
import {Communicator, CommunicatorContext} from '@dbos-inc/dbos-sdk';

class RandomCommunicator
{
    @Communicator()
    static random(_ctx: CommunicatorContext) : Promise<number> {
        return Promise.resolve(Math.random());
    }
}

export
{
    RandomCommunicator,
    RandomCommunicator as RandomStep,
}
