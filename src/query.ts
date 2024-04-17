import { PoolClient } from "pg";

export interface DBOSFieldDef {
    name: string;
    tableID: number;
    columnID: number;
    dataTypeID: number;
    dataTypeSize: number;
    dataTypeModifier: number;
    format: string;
}

export interface DBOSQueryResultBase {
    command: string;
    rowCount: number | null;
    oid: number;
    fields: DBOSFieldDef[];
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export interface DBOSQueryResult<R extends DBOSQueryResultRow = any> extends DBOSQueryResultBase {
    rows: R[];
}

export interface DBOSQueryResultRow {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    [column: string]: any;
}

export type DBOSQueryConfigValues<T> = T extends Array<infer U> ? (U extends never ? never : T) : never;

export interface DBOSQuery {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    query<R extends DBOSQueryResultRow = any, I = any[]>(
        queryText: string,
        values?: DBOSQueryConfigValues<I>
    ): Promise<DBOSQueryResult<R>>;
}

export class DBOSQueryImpl implements DBOSQuery {

    constructor(private readonly client: PoolClient) { }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    query<R extends DBOSQueryResultRow = any, I = any[]>(queryText: string, values?: DBOSQueryConfigValues<I> | undefined): Promise<DBOSQueryResult<R>> {
        return this.client.query<R, I>(queryText, values);
    }
}
