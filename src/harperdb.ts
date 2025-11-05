import { HarperDBClient } from './client';
import { chunk } from 'lodash';
import * as fs from 'fs';
import * as path from 'path';
import {
    HarperDBConfig,
    QueryResult,
    BulkOperationResult,
    ParallelQueryResult,
    InsertOptions,
    UpdateOptions,
    UpsertOptions,
    DeleteOptions,
    BulkInsertOptions,
    BulkUpsertOptions,
    BulkDeleteOptions,
    SelectOptions,
    ParallelQueryOptions,
    QueryOptions,
    CreateIndexOptions,
    IndexInfo,
    GraphQLSchemaType,
    ApplySchemaOptions,
} from './types';

export class HarperDB extends HarperDBClient {
    constructor(config: HarperDBConfig) {
        super(config);
    }

    async insert<T = any>(table: string, record: T, options?: InsertOptions): Promise<QueryResult<T>> {
        const startTime = Date.now();
        const schema = options?.hashAttribute ? undefined : this.getSchema();
        const response = await this.executeQuery('insert', {
            schema: schema || this.getSchema(),
            table,
            records: [record],
            ...(options?.hashAttribute && { hash_attribute: options.hashAttribute }),
        }, options);
        this.invalidateTableCache(table, schema || this.getSchema());
        return {
            data: response.data as T,
            metadata: {
                executionTime: Date.now() - startTime,
                recordsAffected: 1,
            },
        };
    }

    async insertMany<T = any>(table: string, records: T[], options?: BulkInsertOptions): Promise<BulkOperationResult> {
        const startTime = Date.now();
        const batchSize = options?.batchSize || 1000;
        const schema = options?.hashAttribute ? undefined : this.getSchema();
        const batches = chunk(records, batchSize);
        let successful = 0;
        let failed = 0;
        const errors: BulkOperationResult['errors'] = [];
        for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
            const batch = batches[batchIndex];
            try {
                await this.executeQuery('insert', {
                    schema: schema || this.getSchema(),
                    table,
                    records: batch,
                    ...(options?.hashAttribute && { hash_attribute: options.hashAttribute }),
                }, options);
                successful += batch.length;
                if (batchIndex === batches.length - 1) {
                    this.invalidateTableCache(table, schema || this.getSchema());
                }
            } catch (error: any) {
                failed += batch.length;
                const startIdx = batchIndex * batchSize;
                batch.forEach((_, idx) => {
                    errors!.push({ index: startIdx + idx, error: error.message || 'Unknown error' });
                });
            }
        }
        return { successful, failed, errors: errors!.length > 0 ? errors! : undefined, executionTime: Date.now() - startTime };
    }

    async insertManyParallel<T = any>(table: string, records: T[], options?: BulkInsertOptions & { concurrency?: number }): Promise<BulkOperationResult> {
        const startTime = Date.now();
        const batchSize = options?.batchSize || 1000;
        const concurrency = (options as any)?.concurrency || this.getConfig().poolSize || 10;
        const schema = options?.hashAttribute ? undefined : this.getSchema();
        const batches = chunk(records, batchSize);
        let successful = 0;
        let failed = 0;
        const errors: BulkOperationResult['errors'] = [];
        for (let i = 0; i < batches.length; i += concurrency) {
            const batchGroup = batches.slice(i, i + concurrency);
            const batchPromises = batchGroup.map(async (batch, batchIndex) => {
                try {
                    await this.executeQuery('insert', {
                        schema: schema || this.getSchema(),
                        table,
                        records: batch,
                        ...(options?.hashAttribute && { hash_attribute: options.hashAttribute }),
                    }, options);
                    successful += batch.length;
                } catch (error: any) {
                    failed += batch.length;
                    const startIdx = (i + batchIndex) * batchSize;
                    batch.forEach((_, idx) => {
                        errors!.push({ index: startIdx + idx, error: error.message || 'Unknown error' });
                    });
                }
            });
            await Promise.all(batchPromises);
        }
        return { successful, failed, errors: errors!.length > 0 ? errors! : undefined, executionTime: Date.now() - startTime };
    }

    async update<T = any>(table: string, record: T, options?: UpdateOptions): Promise<QueryResult<T>> {
        const startTime = Date.now();
        const schema = options?.hashAttribute ? undefined : this.getSchema();
        const response = await this.executeQuery('update', {
            schema: schema || this.getSchema(),
            table,
            records: [record],
            ...(options?.hashAttribute && { hash_attribute: options.hashAttribute }),
        }, options);
        this.invalidateTableCache(table, schema || this.getSchema());
        return {
            data: response.data as T,
            metadata: {
                executionTime: Date.now() - startTime,
                recordsAffected: 1,
            },
        };
    }

    async updateMany<T = any>(table: string, records: T[], options?: BulkInsertOptions): Promise<BulkOperationResult> {
        const startTime = Date.now();
        const batchSize = options?.batchSize || 1000;
        const schema = options?.hashAttribute ? undefined : this.getSchema();
        const batches = chunk(records, batchSize);
        let successful = 0;
        let failed = 0;
        const errors: BulkOperationResult['errors'] = [];
        for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
            const batch = batches[batchIndex];
            try {
                await this.executeQuery('update', {
                    schema: schema || this.getSchema(),
                    table,
                    records: batch,
                    ...(options?.hashAttribute && { hash_attribute: options.hashAttribute }),
                }, options);
                successful += batch.length;
                if (batchIndex === batches.length - 1) {
                    this.invalidateTableCache(table, schema || this.getSchema());
                }
            } catch (error: any) {
                failed += batch.length;
                const startIdx = batchIndex * batchSize;
                batch.forEach((_, idx) => {
                    errors!.push({ index: startIdx + idx, error: error.message || 'Unknown error' });
                });
            }
        }
        return { successful, failed, errors: errors!.length > 0 ? errors! : undefined, executionTime: Date.now() - startTime };
    }

    async upsert<T = any>(table: string, record: T, options?: UpsertOptions): Promise<QueryResult<T>> {
        const startTime = Date.now();
        const schema = options?.hashAttribute ? undefined : this.getSchema();
        const response = await this.executeQuery('upsert', {
            schema: schema || this.getSchema(),
            table,
            records: [record],
            ...(options?.hashAttribute && { hash_attribute: options.hashAttribute }),
        }, options);
        this.invalidateTableCache(table, schema || this.getSchema());
        return {
            data: response.data as T,
            metadata: {
                executionTime: Date.now() - startTime,
                recordsAffected: 1,
            },
        };
    }

    async upsertMany<T = any>(table: string, records: T[], options?: BulkUpsertOptions & { concurrency?: number }): Promise<BulkOperationResult> {
        const startTime = Date.now();
        const batchSize = options?.batchSize || 1000;
        const concurrency = (options as any)?.concurrency || this.getConfig().poolSize || 10;
        const schema = options?.hashAttribute ? undefined : this.getSchema();
        const batches = chunk(records, batchSize);
        let successful = 0;
        let failed = 0;
        const errors: BulkOperationResult['errors'] = [];
        for (let i = 0; i < batches.length; i += concurrency) {
            const batchGroup = batches.slice(i, i + concurrency);
            const batchPromises = batchGroup.map(async (batch, batchIndex) => {
                try {
                    await this.executeQuery('upsert', {
                        schema: schema || this.getSchema(),
                        table,
                        records: batch,
                        ...(options?.hashAttribute && { hash_attribute: options.hashAttribute }),
                    }, options);
                    successful += batch.length;
                } catch (error: any) {
                    failed += batch.length;
                    const startIdx = (i + batchIndex) * batchSize;
                    batch.forEach((_, idx) => {
                        errors!.push({ index: startIdx + idx, error: error.message || 'Unknown error' });
                    });
                }
            });
            await Promise.all(batchPromises);
        }
        this.invalidateTableCache(table, schema || this.getSchema());
        return { successful, failed, errors: errors!.length > 0 ? errors! : undefined, executionTime: Date.now() - startTime };
    }

    async delete(table: string, hashValue: string, options?: DeleteOptions): Promise<QueryResult<{ deleted_hashes: string[] }>> {
        const startTime = Date.now();
        const schema = options?.hashAttribute ? undefined : this.getSchema();
        const response = await this.executeQuery('delete', {
            schema: schema || this.getSchema(),
            table,
            hash_values: [hashValue],
            ...(options?.hashAttribute && { hash_attribute: options.hashAttribute }),
        }, options);
        this.invalidateTableCache(table, schema || this.getSchema());
        return {
            data: (response.data as any) || { deleted_hashes: [hashValue] },
            metadata: {
                executionTime: Date.now() - startTime,
                recordsAffected: 1,
            },
        };
    }

    async deleteMany(table: string, hashValues: string[], options?: BulkDeleteOptions): Promise<BulkOperationResult> {
        const startTime = Date.now();
        const batchSize = options?.batchSize || 1000;
        const schema = options?.hashAttribute ? undefined : this.getSchema();
        const batches = chunk(hashValues, batchSize);
        let successful = 0;
        let failed = 0;
        const errors: BulkOperationResult['errors'] = [];
        for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
            const batch = batches[batchIndex];
            try {
                await this.executeQuery('delete', {
                    schema: schema || this.getSchema(),
                    table,
                    hash_values: batch,
                    ...(options?.hashAttribute && { hash_attribute: options.hashAttribute }),
                }, options);
                successful += batch.length;
                if (batchIndex === batches.length - 1) {
                    this.invalidateTableCache(table, schema || this.getSchema());
                }
            } catch (error: any) {
                failed += batch.length;
                const startIdx = batchIndex * batchSize;
                batch.forEach((_, idx) => {
                    errors!.push({ index: startIdx + idx, error: error.message || 'Unknown error' });
                });
            }
        }
        return { successful, failed, errors: errors!.length > 0 ? errors! : undefined, executionTime: Date.now() - startTime };
    }

    async select<T = any>(table: string, options?: SelectOptions): Promise<QueryResult<T[]>> {
        const startTime = Date.now();
        const schema = this.getSchema();
        if ((options as any)?.sql) {
            const response = await this.executeQuery<T[]>('sql', { sql: (options as any).sql }, options);
            return {
                data: (response.data as any) || [],
                metadata: {
                    executionTime: Date.now() - startTime,
                    recordsAffected: Array.isArray(response.data) ? (response.data as any).length : 0,
                },
            };
        }
        if (options?.where) {
            const response = await this.executeQuery<T[]>('search_by_value', {
                schema,
                table,
                search_attribute: Object.keys(options.where)[0],
                search_value: Object.values(options.where)[0],
                get_attributes: ['*'],
                ...(options?.limit && { limit: options.limit }),
            }, options);
            let data = (response.data as any) || [];
            if (options?.orderBy) {
                data = (data as any).sort((a: any, b: any) => {
                    const aVal = a[options.orderBy!];
                    const bVal = b[options.orderBy!];
                    const direction = options.orderDirection === 'desc' ? -1 : 1;
                    if (aVal < bVal) return -1 * direction;
                    if (aVal > bVal) return 1 * direction;
                    return 0;
                });
            }
            return {
                data,
                metadata: {
                    executionTime: Date.now() - startTime,
                    recordsAffected: (data as any).length,
                },
            };
        }
        let sql = `SELECT * FROM ${schema}.${table}`;
        if (options?.limit) {
            sql += ` LIMIT ${options.limit}`;
        }
        if (options?.offset) {
            sql += ` OFFSET ${options.offset}`;
        }
        if (options?.orderBy) {
            sql += ` ORDER BY ${options.orderBy} ${options.orderDirection === 'desc' ? 'DESC' : 'ASC'}`;
        }
        try {
            const response = await this.executeQuery<T[]>('sql', { sql }, options);
            return {
                data: (response.data as any) || [],
                metadata: {
                    executionTime: Date.now() - startTime,
                    recordsAffected: Array.isArray(response.data) ? (response.data as any).length : 0,
                },
            };
        } catch (error) {
            return {
                data: [],
                metadata: {
                    executionTime: Date.now() - startTime,
                    recordsAffected: 0,
                },
            };
        }
    }

    async sql<T = any>(sqlQuery: string, options?: QueryOptions): Promise<QueryResult<T[]>> {
        const startTime = Date.now();
        const response = await this.executeQuery<T[]>('sql', { sql: sqlQuery }, options);
        return {
            data: (response.data as any) || [],
            metadata: {
                executionTime: Date.now() - startTime,
                recordsAffected: Array.isArray(response.data) ? (response.data as any).length : 0,
            },
        };
    }

    async query<T = any>(sqlQuery: string, options?: QueryOptions): Promise<QueryResult<T[]>> {
        return this.sql<T>(sqlQuery, options);
    }

    async parallel<T = any>(
        queries: Array<{ type: 'select' | 'insert' | 'update' | 'delete' | 'upsert' | 'sql'; table?: string; data?: any; sql?: string; options?: QueryOptions }>,
        options?: ParallelQueryOptions,
    ): Promise<ParallelQueryResult<T>> {
        const startTime = Date.now();
        const concurrency = options?.concurrency || this.getConfig().poolSize || 10;
        const failFast = options?.failFast || false;
        const queryPromises = queries.map((query) => {
            let promise: Promise<any>;
            switch (query.type) {
                case 'select':
                    promise = this.select(query.table!, query.options as any);
                    break;
                case 'insert':
                    promise = this.insert(query.table!, query.data, query.options as any);
                    break;
                case 'update':
                    promise = this.update(query.table!, query.data, query.options as any);
                    break;
                case 'upsert':
                    promise = this.upsert(query.table!, query.data, query.options as any);
                    break;
                case 'delete':
                    promise = this.delete(query.table!, query.data, query.options as any);
                    break;
                case 'sql':
                    promise = this.sql(query.sql!, query.options as any);
                    break;
                default:
                    promise = Promise.reject(new Error(`Unknown query type: ${query.type}`));
            }
            return promise.catch((error) => {
                if (failFast) {
                    throw error;
                }
                return { data: null, error: (error as any).message };
            });
        });
        const results = await Promise.all(queryPromises);
        const successful = results.filter((r: any) => (r as any).error === undefined).length;
        const failed = results.length - successful;
        return {
            results: (results as any).map((r: any) => ({ data: r.data ?? r, metadata: { executionTime: 0 } })),
            executionTime: Date.now() - startTime,
            successful,
            failed,
        };
    }

    async getByHash<T = any>(table: string, hashValue: string, options?: QueryOptions): Promise<QueryResult<T>> {
        const startTime = Date.now();
        const schema = this.getSchema();
        const response = await this.executeQuery<T[]>('search_by_hash', {
            schema,
            table,
            hash_values: [hashValue],
            get_attributes: ['*'],
        }, options);
        const data = Array.isArray(response.data) && (response.data as any).length > 0 ? (response.data as any)[0] : null;
        return {
            data: data as any,
            metadata: {
                executionTime: Date.now() - startTime,
                recordsAffected: data ? 1 : 0,
            },
        };
    }

    async getByHashes<T = any>(table: string, hashValues: string[], options?: QueryOptions): Promise<QueryResult<T[]>> {
        const startTime = Date.now();
        const schema = this.getSchema();
        if (!hashValues || hashValues.length === 0) {
            return { data: [] as any, metadata: { executionTime: Date.now() - startTime, recordsAffected: 0 } };
        }
        const response = await this.executeQuery<T[]>('search_by_hash', {
            schema,
            table,
            hash_values: hashValues,
            get_attributes: ['*'],
        }, options);
        return {
            data: (response.data as any) || [],
            metadata: {
                executionTime: Date.now() - startTime,
                recordsAffected: Array.isArray(response.data) ? (response.data as any).length : 0,
            },
        };
    }

    async searchByValue<T = any>(table: string, searchAttribute: string, searchValue: any, getAttributes: string[] = ['*'], options?: QueryOptions): Promise<QueryResult<T[]>> {
        const startTime = Date.now();
        const schema = this.getSchema();
        const response = await this.executeQuery<T[]>('search_by_value', {
            schema,
            table,
            search_attribute: searchAttribute,
            search_value: searchValue,
            get_attributes: getAttributes,
        }, options);
        return {
            data: (response.data as any) || [],
            metadata: {
                executionTime: Date.now() - startTime,
                recordsAffected: Array.isArray(response.data) ? (response.data as any).length : 0,
            },
        };
    }

    async describeTable(table: string, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const schema = this.getSchema();
        const response = await this.executeQuery<any>('describe_table', { schema, table }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async dropTable(table: string, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const schema = this.getSchema();
        const response = await this.executeQuery<any>('drop_table', { schema, table }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async createTable(table: string, hashAttribute: string = 'id', options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const schema = this.getSchema();
        const response = await this.executeQuery<any>('create_table', { schema, table, hash_attribute: hashAttribute }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async createSchema(schema: string, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any>('create_schema', { schema }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async dropSchema(schema: string, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any>('drop_schema', { schema }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async listSchemas(options?: QueryOptions): Promise<QueryResult<string[]>> {
        const startTime = Date.now();
        const response = await this.executeQuery<string[]>('list_schemas', {}, options);
        return { data: (response.data as any) || [], metadata: { executionTime: Date.now() - startTime } };
    }

    async listTables(schema?: string, options?: QueryOptions): Promise<QueryResult<string[]>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        const response = await this.executeQuery<string[]>('list_tables', { schema: schemaName }, options);
        return { data: (response.data as any) || [], metadata: { executionTime: Date.now() - startTime } };
    }

    async systemInformation(options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any>('system_information', {}, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async addUser(username: string, password: string, role: 'super_user' | 'cluster_admin' | 'user' | 'read_only' = 'user', active: boolean = true, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any>('add_user', { username, password, role, active }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async dropUser(username: string, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any>('drop_user', { username }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async alterUser(
        username: string,
        updates: { password?: string; role?: 'super_user' | 'cluster_admin' | 'user' | 'read_only'; active?: boolean },
        options?: QueryOptions,
    ): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any>('alter_user', { username, ...updates }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async listUsers(options?: QueryOptions): Promise<QueryResult<any[]>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any[]>('list_users', {}, options);
        return { data: (response.data as any) || [], metadata: { executionTime: Date.now() - startTime } };
    }

    async listAttributes(table: string, schema?: string, options?: QueryOptions): Promise<QueryResult<string[]>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        try {
            const response = await this.executeQuery<string[]>('list_attributes', { schema: schemaName, table }, options);
            return { data: (response.data as any) || [], metadata: { executionTime: Date.now() - startTime } };
        } catch (error) {
            try {
                const tableDesc = await this.describeTable(table, options);
                const tableData = tableDesc.data;
                let attributes: string[] = [];
                if (tableData && typeof tableData === 'object') {
                    if (Array.isArray((tableData as any).attributes)) {
                        attributes = (tableData as any).attributes.map((attr: any) => attr.name || attr.attribute || attr);
                    } else if ((tableData as any).hash_attribute) {
                        attributes = [(tableData as any).hash_attribute];
                    }
                }
                return { data: attributes, metadata: { executionTime: Date.now() - startTime } };
            } catch {
                return { data: [], metadata: { executionTime: Date.now() - startTime } };
            }
        }
    }

    async addAttribute(table: string, attribute: string, schema?: string, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        const response = await this.executeQuery<any>('add_attribute', { schema: schemaName, table, attribute }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async dropAttribute(table: string, attribute: string, schema?: string, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        const response = await this.executeQuery<any>('drop_attribute', { schema: schemaName, table, attribute }, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async createIndex(table: string, attribute: string, options?: CreateIndexOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const schema = this.getSchema();
        const exists = await this.indexExists(table, attribute, schema);
        if (exists) {
            return { data: { message: `Index on attribute '${attribute}' already exists` }, metadata: { executionTime: Date.now() - startTime } } as any;
        }
        const payload: any = { schema, table, attribute, hash_attribute: false };
        if (options?.unique) payload.unique = true;
        if (options?.indexName) payload.name = options.indexName;
        const response = await this.executeQuery<any>('add_attribute', payload, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async dropIndex(table: string, attribute: string, schema?: string, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        const exists = await this.indexExists(table, attribute, schemaName);
        if (!exists) {
            return { data: { message: `Index on attribute '${attribute}' does not exist` }, metadata: { executionTime: Date.now() - startTime } } as any;
        }
        return this.dropAttribute(table, attribute, schemaName, options);
    }

    async listIndexes(table: string, schema?: string, options?: QueryOptions): Promise<QueryResult<IndexInfo[]>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        try {
            const tableDesc = await this.describeTable(table, options);
            const tableData = tableDesc.data as any;
            let indexes: IndexInfo[] = [];
            if (tableData && typeof tableData === 'object') {
                if (Array.isArray(tableData.indexes)) {
                    indexes = tableData.indexes.map((idx: any) => ({ attribute: idx.attribute || idx.name, name: idx.name, unique: idx.unique || false }));
                } else if (Array.isArray(tableData.attributes)) {
                    indexes = (tableData.attributes as any)
                        .filter((attr: any) => attr.indexed || (attr.hash_attribute === false && attr.attribute !== tableData.hash_attribute))
                        .map((attr: any) => ({ attribute: attr.attribute || attr.name, name: attr.name, unique: attr.unique || false }));
                } else {
                    try {
                        const sqlResult = await this.sql<any>(`SHOW INDEXES FROM \`${schemaName}\`.\`${table}\``, options);
                        if (Array.isArray(sqlResult.data)) {
                            indexes = (sqlResult.data as any).map((idx: any) => ({
                                attribute: idx.Column_name || idx.column_name,
                                name: idx.Key_name || idx.key_name,
                                unique: idx.Non_unique === 0 || idx.non_unique === false,
                            }));
                        }
                    } catch {}
                }
            }
            return { data: indexes, metadata: { executionTime: Date.now() - startTime } };
        } catch {
            return { data: [], metadata: { executionTime: Date.now() - startTime } };
        }
    }

    async indexExists(table: string, attribute: string, schema?: string, options?: QueryOptions): Promise<boolean> {
        try {
            const indexes = await this.listIndexes(table, schema, options);
            if (Array.isArray(indexes.data)) {
                return (indexes.data as any).some((idx: any) => idx.attribute === attribute || idx.name === attribute);
            }
            return false;
        } catch {
            return false;
        }
    }

    async clusterStatus(options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any>('cluster_status', {}, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async nodeStatus(options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any>('node_status', {}, options);
        return { data: response.data, metadata: { executionTime: Date.now() - startTime } };
    }

    async readLog(limit: number = 100, start?: number, options?: QueryOptions): Promise<QueryResult<any[]>> {
        const startTime = Date.now();
        const response = await this.executeQuery<any[]>('read_log', { limit, ...(start !== undefined && { start }) }, options);
        return { data: (response.data as any) || [], metadata: { executionTime: Date.now() - startTime } };
    }

    async deleteByValue(
        table: string,
        searchAttribute: string,
        searchValue: any,
        schema?: string,
        options?: QueryOptions,
    ): Promise<QueryResult<{ deleted_hashes: string[] }>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        const findResult = await this.searchByValue<any>(table, searchAttribute, searchValue, ['id'], options);
        if (!Array.isArray(findResult.data) || findResult.data.length === 0) {
            return { data: { deleted_hashes: [] }, metadata: { executionTime: Date.now() - startTime, recordsAffected: 0 } };
        }
        const hashValues = (findResult.data as any).map((record: any) => record.id || record[searchAttribute]).filter(Boolean);
        if (hashValues.length === 0) {
            return { data: { deleted_hashes: [] }, metadata: { executionTime: Date.now() - startTime, recordsAffected: 0 } };
        }
        const deleteResult = await this.deleteMany(table, hashValues, options);
        this.invalidateTableCache(table, schemaName);
        return { data: { deleted_hashes: hashValues }, metadata: { executionTime: Date.now() - startTime, recordsAffected: deleteResult.successful } };
    }

    async getTableSchema(table: string, schema?: string, options?: QueryOptions): Promise<QueryResult<any>> {
        return this.describeTable(table, options);
    }

    async tableExists(table: string, schema?: string, options?: QueryOptions): Promise<boolean> {
        try { await this.describeTable(table, options); return true; } catch { return false; }
    }

    async schemaExists(schema: string, options?: QueryOptions): Promise<boolean> {
        try {
            const schemas = await this.listSchemas(options);
            return Array.isArray(schemas.data) && (schemas.data as any).includes(schema);
        } catch { return false; }
    }

    async promiseAll<T = any>(promises: Array<Promise<any>>, options?: { failFast?: boolean }): Promise<Array<{ success: boolean; data?: T; error?: string }>> {
        try {
            if (options?.failFast) {
                const results = await Promise.all(promises);
                return results.map((data) => ({ success: true, data }));
            } else {
                const results = await Promise.allSettled(promises);
                return results.map((result) => result.status === 'fulfilled' ? { success: true, data: result.value } : { success: false, error: (result as any).reason?.message || 'Unknown error' });
            }
        } catch (error: any) {
            throw new Error(`Promise.all failed: ${error.message}`);
        }
    }

    async batch<T = any>(operations: Array<() => Promise<any>>, options?: { concurrency?: number; failFast?: boolean }): Promise<Array<{ success: boolean; data?: T; error?: string }>> {
        const concurrency = options?.concurrency || this.getConfig().poolSize || 10;
        const failFast = options?.failFast || false;
        const results: Array<{ success: boolean; data?: T; error?: string }> = [];
        for (let i = 0; i < operations.length; i += concurrency) {
            const batch = operations.slice(i, i + concurrency);
            const batchPromises = batch.map((op) => op());
            if (failFast) {
                const batchResults = await Promise.all(batchPromises);
                results.push(...batchResults.map((data) => ({ success: true, data })));
            } else {
                const batchResults = await Promise.allSettled(batchPromises);
                results.push(...batchResults.map((result) => result.status === 'fulfilled' ? { success: true, data: (result as any).value } : { success: false, error: (result as any).reason?.message || 'Unknown error' }));
            }
        }
        return results;
    }

    async count(table: string, condition?: Record<string, any>, schema?: string, options?: QueryOptions): Promise<QueryResult<number>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        if (condition && Object.keys(condition).length > 0) {
            const searchAttr = Object.keys(condition)[0];
            const searchVal = Object.values(condition)[0];
            const result = await this.searchByValue<any>(table, searchAttr, searchVal, ['*'], options);
            return { data: Array.isArray(result.data) ? (result.data as any).length : 0, metadata: { executionTime: Date.now() - startTime } };
        }
        try {
            const desc = await this.describeTable(table, options);
            const recordCount = (desc.data as any)?.record_count || 0;
            return { data: recordCount, metadata: { executionTime: Date.now() - startTime } };
        } catch {
            const result = await this.select<any>(table, options);
            return { data: Array.isArray(result.data) ? (result.data as any).length : 0, metadata: { executionTime: Date.now() - startTime } };
        }
    }

    async exists(table: string, hashValue: string, options?: QueryOptions): Promise<boolean> {
        try { const result = await this.getByHash<any>(table, hashValue, options); return result.data !== null && result.data !== undefined; } catch { return false; }
    }

    async findOne<T = any>(table: string, condition: Record<string, any>, options?: SelectOptions): Promise<QueryResult<T | null>> {
        const startTime = Date.now();
        const result = await this.select<T>(table, { ...(options as any), where: condition, limit: 1 });
        return { data: Array.isArray(result.data) && result.data.length > 0 ? (result.data as any)[0] : null, metadata: { executionTime: Date.now() - startTime, recordsAffected: (result.data ? 1 : 0) as any } };
    }

    async findMany<T = any>(table: string, condition: Record<string, any>, options?: SelectOptions): Promise<QueryResult<T[]>> {
        return this.select<T>(table, { ...(options as any), where: condition });
    }

    async updateByValue(
        table: string,
        searchAttribute: string,
        searchValue: any,
        updates: Record<string, any>,
        schema?: string,
        options?: QueryOptions,
    ): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        const setClause = Object.keys(updates).map((key) => `${key} = '${updates[key]}'`).join(', ');
        const sql = `UPDATE ${schemaName}.${table} SET ${setClause} WHERE ${searchAttribute} = '${searchValue}'`;
        const response = await this.sql<any>(sql, options);
        this.invalidateTableCache(table, schemaName);
        return { data: response.data as any, metadata: { executionTime: Date.now() - startTime } };
    }

    async paginate<T = any>(table: string, options?: { page?: number; pageSize?: number } & SelectOptions): Promise<QueryResult<{ data: T[]; page: number; pageSize: number; total: number; totalPages: number }>> {
        const startTime = Date.now();
        const page = options?.page || 1;
        const pageSize = options?.pageSize || 10;
        const offset = (page - 1) * pageSize;
        const totalResult = await this.count(table, options?.where);
        const total = totalResult.data || 0;
        const totalPages = Math.ceil(total / pageSize);
        const dataResult = await this.select<T>(table, { ...(options as any), limit: pageSize, offset });
        return { data: { data: Array.isArray(dataResult.data) ? (dataResult.data as any) : [], page, pageSize, total, totalPages }, metadata: { executionTime: Date.now() - startTime } };
    }

    async clearTable(table: string, options?: QueryOptions): Promise<QueryResult<any>> {
        const startTime = Date.now();
        const allRecords = await this.select<any>(table, options);
        if (Array.isArray(allRecords.data) && allRecords.data.length > 0) {
            const hashAttribute = 'id';
            const hashValues = (allRecords.data as any).map((record: any) => record[hashAttribute]).filter(Boolean);
            if (hashValues.length > 0) {
                const deleteResult = await this.deleteMany(table, hashValues, options);
                return { data: { deleted_count: deleteResult.successful }, metadata: { executionTime: Date.now() - startTime, recordsAffected: deleteResult.successful } } as any;
            }
        }
        return { data: { deleted_count: 0 }, metadata: { executionTime: Date.now() - startTime, recordsAffected: 0 } } as any;
    }

    async copyTable(sourceTable: string, targetTable: string, srcSchema?: string, tgtSchema?: string, options?: QueryOptions): Promise<QueryResult<{ source_table: string; target_table: string; records_copied: number }>> {
        const startTime = Date.now();
        const sourceSchema = srcSchema || this.getSchema();
        const targetSchema = tgtSchema || this.getSchema();
        const sourceDesc = await this.describeTable(sourceTable, options);
        const hashAttribute = (sourceDesc.data as any)?.hash_attribute || 'id';
        await this.createTable(targetTable, hashAttribute, options);
        const sourceData = await this.select<any>(sourceTable, { schema: sourceSchema, ...(options as any) });
        if (Array.isArray(sourceData.data) && sourceData.data.length > 0) {
            await this.insertMany(targetTable, sourceData.data as any, { schema: targetSchema, ...(options as any) } as any);
        }
        return { data: { source_table: sourceTable, target_table: targetTable, records_copied: Array.isArray(sourceData.data) ? (sourceData.data as any).length : 0 }, metadata: { executionTime: Date.now() - startTime } };
    }

    async healthCheck(options?: QueryOptions): Promise<QueryResult<{ status: string; timestamp: number }>> {
        const startTime = Date.now();
        try {
            await this.systemInformation(options);
            return { data: { status: 'healthy', timestamp: Date.now() }, metadata: { executionTime: Date.now() - startTime } };
        } catch {
            return { data: { status: 'unhealthy', timestamp: Date.now() }, metadata: { executionTime: Date.now() - startTime } };
        }
    }

    async testConnection(options?: QueryOptions): Promise<boolean> {
        try { await this.systemInformation(options); return true; } catch { return false; }
    }

    getTableStats(table: string, schema?: string, options?: QueryOptions): Promise<QueryResult<{ recordCount: number; attributes: string[] }>> {
        const startTime = Date.now();
        const schemaName = schema || this.getSchema();
        return Promise.all([
            this.count(table, undefined, schemaName, options),
            this.listAttributes(table, schemaName, options),
        ]).then(([countResult, attributesResult]) => ({
            data: { recordCount: countResult.data || 0, attributes: Array.isArray(attributesResult.data) ? (attributesResult.data as any) : [] },
            metadata: { executionTime: Date.now() - startTime },
        }));
    }

    async search<T = any>(table: string, condition: Record<string, any>, options?: QueryOptions): Promise<QueryResult<T[]>> {
        const searchAttr = Object.keys(condition)[0];
        const searchVal = Object.values(condition)[0];
        return this.searchByValue<T>(table, searchAttr, searchVal, ['*'], options);
    }

    async transaction<T = any>(operations: Array<() => Promise<any>>, options?: { rollbackOnError?: boolean }): Promise<QueryResult<{ success: boolean; results: T[]; error?: string }>> {
        const startTime = Date.now();
        const rollbackOnError = options?.rollbackOnError || false;
        const results: T[] = [];
        try {
            for (const operation of operations) {
                const result = await operation();
                results.push(result);
            }
            return { data: { success: true, results }, metadata: { executionTime: Date.now() - startTime } };
        } catch (error: any) {
            if (rollbackOnError) {
                // no-op simulated
            }
            return { data: { success: false, results, error: error.message || 'Unknown error' }, metadata: { executionTime: Date.now() - startTime } };
        }
    }

    clearCache(): void { super.clearCache(); }
    invalidateTableCache(table: string, schema?: string): void { super.invalidateTableCache(table, schema); }

    parseGraphQLSchema(sdlContent: string): GraphQLSchemaType[] {
        const types: GraphQLSchemaType[] = [];
        const normalized = sdlContent.replace(/\r\n?/g, '\n');
        const typeRegex = /type\s+(\w+)\s+([^ {]*)\{([\s\S]*?)\}/g;
        let match: RegExpExecArray | null;
        while ((match = typeRegex.exec(normalized)) !== null) {
            const typeName = match[1];
            const headerDirectives = (match[2] || '').trim();
            const body = match[3] || '';
            const tableMatch = /@table\s*\(([^)]+)\)/.exec(headerDirectives);
            if (!tableMatch) continue;
            const tableArgs = tableMatch[1];
            const databaseMatch = /database\s*:\s*"([^"]+)"/.exec(tableArgs);
            const replicateMatch = /replicate\s*:\s*(false|true)/.exec(tableArgs);
            if (!databaseMatch) continue;
            const schema = databaseMatch[1];
            const replicate = replicateMatch ? replicateMatch[1] === 'true' : true;
            const fields: GraphQLSchemaType['fields'] = [];
            let primaryKey = '';
            const lines = body.split('\n').map(l => l.trim()).filter(l => l && !l.startsWith('#'));
            for (const line of lines) {
                const fieldMatch = /^(\w+)\s*:\s*([\w\[\]!]+)(.*)$/.exec(line);
                if (!fieldMatch) continue;
                const fieldName = fieldMatch[1];
                const fieldType = fieldMatch[2];
                const fieldDirectives = (fieldMatch[3] || '').trim();
                const isPrimaryKey = /@primaryKey\b/.test(fieldDirectives);
                const isIndexed = /@indexed\b/.test(fieldDirectives);
                const isRequired = fieldType.endsWith('!');
                if (isPrimaryKey) primaryKey = fieldName;
                fields.push({ name: fieldName, type: fieldType.replace(/[!\[\]]/g, ''), isPrimaryKey, isIndexed, isRequired });
            }
            if (!primaryKey) {
                const idField = fields.find(f => f.name === 'id');
                if (idField) { primaryKey = 'id'; (idField as any).isPrimaryKey = true; }
            }
            if (!primaryKey) continue;
            types.push({ name: typeName, schema, hashAttribute: primaryKey, fields, replicate });
        }
        return types;
    }

    validateSchemaFile(filePath: string): { valid: boolean; error?: string } {
        const absolutePath = path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
        if (!fs.existsSync(absolutePath)) {
            return { valid: false, error: `Schema file not found: ${absolutePath}` };
        }
        const ext = path.extname(absolutePath).toLowerCase();
        if (ext !== '.graphql' && ext !== '.gql') {
            return { valid: false, error: `Invalid file extension. Expected .graphql or .gql, got: ${ext || 'no extension'}` };
        }
        const stats = fs.statSync(absolutePath);
        if (stats.size === 0) {
            return { valid: false, error: `Schema file is empty: ${absolutePath}` };
        }
        try {
            const sdlContent = fs.readFileSync(absolutePath, 'utf8');
            if (!sdlContent.includes('type ')) {
                return { valid: false, error: `Invalid GraphQL schema file. File does not contain 'type' definitions.` };
            }
            if (!sdlContent.includes('@table')) {
                return { valid: false, error: `Invalid GraphQL schema file. File does not contain '@table' directive. HarperDB requires @table directive to identify tables.` };
            }
            try {
                const types = this.parseGraphQLSchema(sdlContent);
                if (types.length === 0) {
                    return { valid: false, error: `Invalid GraphQL schema file. No valid table types found. Make sure your types have @table(database: "schema_name") directive.` };
                }
            } catch (parseError: any) {
                return { valid: false, error: `Invalid GraphQL schema format: ${parseError.message}` };
            }
            return { valid: true };
        } catch (readError: any) {
            return { valid: false, error: `Failed to read schema file: ${readError.message}` };
        }
    }

    async loadSchemaFromFile(filePath: string): Promise<GraphQLSchemaType[]> {
        const validation = this.validateSchemaFile(filePath);
        if (!validation.valid) {
            throw new Error(validation.error || 'Invalid schema file');
        }
        const absolutePath = path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
        const sdlContent = fs.readFileSync(absolutePath, 'utf8');
        return this.parseGraphQLSchema(sdlContent);
    }

    async applySchema(schemaContent: string | GraphQLSchemaType[], options?: ApplySchemaOptions): Promise<QueryResult<{ schemasCreated: string[]; tablesCreated: string[]; attributesCreated: string[]; indexesCreated: string[]; errors: Array<{ type: string; name: string; error: string }> }>> {
        const startTime = Date.now();
        const schemasCreated = new Set<string>();
        const tablesCreated: string[] = [];
        const attributesCreated: string[] = [];
        const indexesCreated: string[] = [];
        const errors: Array<{ type: string; name: string; error: string }> = [];
        let types: GraphQLSchemaType[];
        if (typeof schemaContent === 'string') {
            types = this.parseGraphQLSchema(schemaContent);
        } else {
            types = schemaContent;
        }
        for (const type of types) {
            try {
                if (!schemasCreated.has(type.schema)) {
                    try {
                        const exists = await this.schemaExists(type.schema);
                        if (!exists) {
                            await this.createSchema(type.schema, options);
                        }
                        schemasCreated.add(type.schema);
                    } catch (error: any) {
                        if (error.message?.includes('already exists')) {
                            schemasCreated.add(type.schema);
                        } else {
                            throw error;
                        }
                    }
                }
                const originalSchema = this.getSchema();
                let tableExists = false;
                try {
                    this.setSchema(type.schema);
                    tableExists = await this.tableExists(type.name);
                } finally {
                    this.setSchema(originalSchema);
                }
                if (tableExists && options?.force) {
                    this.setSchema(type.schema);
                    await this.dropTable(type.name, options);
                    this.setSchema(originalSchema);
                }
                if (tableExists && options?.skipExisting) {
                    continue;
                }
                if (!tableExists || options?.force) {
                    try {
                        this.setSchema(type.schema);
                        await this.createTable(type.name, type.hashAttribute, options);
                        tablesCreated.push(`${type.schema}.${type.name}`);
                        const sampleRecord: Record<string, any> = {};
                        for (const field of type.fields) {
                            if (field.name === type.hashAttribute) {
                                if (field.type === 'ID' || field.type === 'String') {
                                    sampleRecord[field.name] = `__temp_schema_init__`;
                                } else if (field.type === 'Long' || field.type === 'Int') {
                                    sampleRecord[field.name] = 0;
                                } else {
                                    sampleRecord[field.name] = null;
                                }
                            } else {
                                if (field.isRequired) {
                                    if (field.type === 'String') {
                                        sampleRecord[field.name] = '';
                                    } else if (field.type === 'Int' || field.type === 'Long') {
                                        sampleRecord[field.name] = 0;
                                    } else if (field.type === 'Boolean') {
                                        sampleRecord[field.name] = false;
                                    } else if (field.type === 'Date') {
                                        sampleRecord[field.name] = new Date().toISOString();
                                    } else {
                                        sampleRecord[field.name] = null;
                                    }
                                } else {
                                    sampleRecord[field.name] = null;
                                }
                            }
                        }
                        try {
                            await this.insert(type.name, sampleRecord as any, { hashAttribute: type.hashAttribute, ...(options as any) });
                            attributesCreated.push(...type.fields.map(f => `${type.schema}.${type.name}.${f.name}`));
                            try { await this.delete(type.name, (sampleRecord as any)[type.hashAttribute], options as any); } catch {}
                        } catch {}
                    } finally {
                        this.setSchema(originalSchema);
                    }
                }
                for (const field of type.fields) {
                    if (field.isIndexed && field.name !== type.hashAttribute) {
                        try {
                            try {
                                this.setSchema(type.schema);
                                const indexResult = await this.createIndex(type.name, field.name, { timeout: options?.timeout, retries: (options as any)?.retries });
                                if ((indexResult as any).data && !((indexResult as any).data.message?.includes('already exists'))) {
                                    indexesCreated.push(`${type.schema}.${type.name}.${field.name}`);
                                }
                            } finally {
                                this.setSchema(originalSchema);
                            }
                        } catch (error: any) {
                            errors.push({ type: 'index', name: `${type.schema}.${type.name}.${field.name}`, error: error.message || 'Unknown error' });
                        }
                    }
                }
            } catch (error: any) {
                errors.push({ type: 'table', name: `${type.schema}.${type.name}`, error: error.message || 'Unknown error' });
            }
        }
        return { data: { schemasCreated: Array.from(schemasCreated), tablesCreated, attributesCreated, indexesCreated, errors }, metadata: { executionTime: Date.now() - startTime } };
    }

    async applySchemaFromFile(filePath: string, options?: ApplySchemaOptions): Promise<QueryResult<{ schemasCreated: string[]; tablesCreated: string[]; attributesCreated: string[]; indexesCreated: string[]; errors: Array<{ type: string; name: string; error: string }> }>> {
        const validation = this.validateSchemaFile(filePath);
        if (!validation.valid) {
            throw new Error(validation.error || 'Invalid schema file');
        }
        const types = await this.loadSchemaFromFile(filePath);
        return this.applySchema(types, options);
    }
}


