import { DBOS } from "@dbos-inc/dbos-sdk";

export class fooWorkflowClass {

    @DBOS.transaction()
    static async fooDBOS(userName: string) {
        DBOS.logger.info("Hello from DBOS foooooo!");
        
    }

}