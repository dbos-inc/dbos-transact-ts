import { Communicator, CommunicatorContext, DBOS } from '@dbos-inc/dbos-sdk';

class CurrentTimeCommunicator {
  @Communicator()
  static getCurrentDate(_ctx: CommunicatorContext): Promise<Date> {
    return Promise.resolve(new Date());
  }

  @Communicator()
  static getCurrentTime(_ctx: CommunicatorContext): Promise<number> {
    return Promise.resolve(new Date().getTime());
  }
}

class DBOSDateTime {
  @DBOS.step()
  static getCurrentDate(): Promise<Date> {
    return Promise.resolve(new Date());
  }

  @DBOS.step()
  static getCurrentTime(): Promise<number> {
    return Promise.resolve(new Date().getTime());
  }
}

export { CurrentTimeCommunicator, CurrentTimeCommunicator as CurrentTimeStep, DBOSDateTime };
