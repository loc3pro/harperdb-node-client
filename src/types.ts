export interface HarperDBConfig {
    url: string;
    username: string;
    password: string;
    schema?: string;
    timeout?: number;
    maxRetries?: number;
    retryDelay?: number;
    poolSize?: number;
    apiPath?: string;
    enableCache?: boolean;
    cacheTTL?: number;
    maxCacheSize?: number;
    keepAlive?: boolean;
    keepAliveMsecs?: number;
    maxSockets?: number;
    maxFreeSockets?: number;
}

export interface QueryOptions {
    timeout?: number;
    retries?: number;
    useCache?: boolean;
    cacheTTL?: number;
}

export interface InsertOptions extends QueryOptions {
    hashAttribute?: string;
}

export interface UpdateOptions extends QueryOptions {
    hashAttribute?: string;
}

export interface UpsertOptions extends QueryOptions {
    hashAttribute?: string;
}

export interface DeleteOptions extends QueryOptions {
    hashAttribute?: string;
}

export interface BulkInsertOptions extends InsertOptions {
    batchSize?: number;
}

export interface BulkUpsertOptions extends UpsertOptions {
    batchSize?: number;
}

export interface BulkDeleteOptions extends DeleteOptions {
    batchSize?: number;
}

export interface SelectOptions extends QueryOptions {
    limit?: number;
    offset?: number;
    where?: Record<string, any>;
    orderBy?: string;
    orderDirection?: 'asc' | 'desc';
    sql?: string;
}

export interface ParallelQueryOptions {
    concurrency?: number;
    failFast?: boolean;
}

export interface HarperDBResponse<T = any> {
    message: string;
    data?: T;
    error?: string;
    metadata?: {
        executionTime?: number;
        cached?: boolean;
    };
}

export interface QueryResult<T = any> {
    data: T;
    metadata?: {
        executionTime: number;
        recordsAffected?: number;
    };
}

export interface BulkOperationResult {
    successful: number;
    failed: number;
    errors?: Array<{
        index: number;
        error: string;
    }>;
    executionTime: number;
}

export interface ParallelQueryResult<T = any> {
    results: Array<QueryResult<T>>;
    executionTime: number;
    failed: number;
    successful: number;
}

export interface UserOptions extends QueryOptions {
    username: string;
    password: string;
    role?: 'super_user' | 'cluster_admin' | 'user' | 'read_only';
    active?: boolean;
}

export interface AlterUserOptions extends QueryOptions {
    username: string;
    password?: string;
    role?: 'super_user' | 'cluster_admin' | 'user' | 'read_only';
    active?: boolean;
}

export interface AddAttributeOptions extends QueryOptions {
    attribute: string;
    hashAttribute?: string;
}

export interface CreateIndexOptions extends QueryOptions {
    unique?: boolean;
    indexName?: string;
}

export interface IndexInfo {
    attribute: string;
    name?: string;
    unique?: boolean;
}

export interface GraphQLSchemaField {
    name: string;
    type: string;
    isPrimaryKey: boolean;
    isIndexed: boolean;
    isRequired: boolean;
}

export interface GraphQLSchemaType {
    name: string;
    schema: string;
    hashAttribute: string;
    fields: GraphQLSchemaField[];
    replicate?: boolean;
}

export interface ApplySchemaOptions extends QueryOptions {
    force?: boolean;
    skipExisting?: boolean;
}


