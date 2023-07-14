import { Pool, PoolConfig } from 'pg';

export class Operon {
  pool: Pool;
  constructor(config: PoolConfig) {
    this.pool = new Pool(config)
  }
}