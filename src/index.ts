export * from './harperdb';
export * from './client';
export * from './types';
export * from './query-builder';

import { HarperDB } from './harperdb';
import { HarperDBConfig } from './types';

export function createClient(config: HarperDBConfig): HarperDB {
    return new HarperDB(config);
}

export default HarperDB;


