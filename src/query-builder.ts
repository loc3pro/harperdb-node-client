import { HarperDB } from './harperdb';
import { QueryResult, SelectOptions } from './types';

export class QueryBuilder {
    private harperDB: HarperDB;
    private table: string;
    private options: SelectOptions = {};

    constructor(harperDB: HarperDB, table: string) {
        this.harperDB = harperDB;
        this.table = table;
    }

    where(condition: Record<string, any>): this {
        this.options.where = condition;
        return this;
    }

    limit(count: number): this {
        this.options.limit = count;
        return this;
    }

    offset(count: number): this {
        this.options.offset = count;
        return this;
    }

    orderBy(field: string, direction: 'asc' | 'desc' = 'asc'): this {
        this.options.orderBy = field;
        this.options.orderDirection = direction;
        return this;
    }

    async execute<T = any>(): Promise<QueryResult<T[]>> {
        return this.harperDB.select<T>(this.table, this.options);
    }
}


