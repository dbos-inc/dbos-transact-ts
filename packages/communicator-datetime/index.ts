
import {Communicator, CommunicatorContext} from '@dbos-inc/dbos-sdk';

class CurrentTimeCommunicator
{
    @Communicator()
    static getCurrentDate(_ctx: CommunicatorContext) : Promise<Date> {
        return Promise.resolve(new Date());
    }

    @Communicator()
    static getCurrentTime(_ctx: CommunicatorContext) : Promise<number> {
        return Promise.resolve(new Date().getUTCMilliseconds());
    }
}

export
{
    CurrentTimeCommunicator
}