// Sleep for specified milliseconds.
export const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

export interface TestKvTable {
  id?: number,
  value?: string,
}