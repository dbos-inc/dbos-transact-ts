import { PoolConfig } from "pg";
import { ConfigFile } from "./config";

export function db_wizard(poolConfig: PoolConfig): PoolConfig {
    console.log("WIZARd")
    return poolConfig
}