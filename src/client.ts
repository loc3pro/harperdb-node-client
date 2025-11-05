import axios, { AxiosInstance, AxiosRequestConfig } from 'axios';
import * as http from 'http';
import * as https from 'https';
import { HarperDBConfig, QueryOptions, HarperDBResponse } from './types';

type CacheEntry = {
    data: HarperDBResponse<any>;
    timestamp: number;
    ttl: number;
};

export class HarperDBClient {
    private axiosInstance: AxiosInstance;
    private config: HarperDBConfig;
    private requestQueue: Array<() => Promise<void>> = [];
    private isProcessingQueue = false;
    private maxConcurrentRequests: number;
    private queryCache: Map<string, CacheEntry> = new Map();
    private httpAgent?: http.Agent;
    private httpsAgent?: https.Agent;

    constructor(config: HarperDBConfig) {
        this.config = {
            schema: config.schema || 'dev',
            timeout: config.timeout || 30000,
            maxRetries: config.maxRetries || 3,
            retryDelay: config.retryDelay || 1000,
            poolSize: config.poolSize || 10,
            apiPath: config.apiPath || '',
            enableCache: config.enableCache ?? false,
            cacheTTL: config.cacheTTL || 5000,
            maxCacheSize: config.maxCacheSize || 1000,
            keepAlive: config.keepAlive !== false,
            keepAliveMsecs: config.keepAliveMsecs || 1000,
            maxSockets: config.maxSockets || 50,
            maxFreeSockets: config.maxFreeSockets || 10,
            ...config,
        } as HarperDBConfig;

        this.maxConcurrentRequests = this.config.poolSize || 10;

        if (this.config.keepAlive) {
            this.httpAgent = new http.Agent({
                keepAlive: true,
                keepAliveMsecs: this.config.keepAliveMsecs,
                maxSockets: this.config.maxSockets,
                maxFreeSockets: this.config.maxFreeSockets,
            });
            this.httpsAgent = new https.Agent({
                keepAlive: true,
                keepAliveMsecs: this.config.keepAliveMsecs,
                maxSockets: this.config.maxSockets,
                maxFreeSockets: this.config.maxFreeSockets,
            });
        }

        this.axiosInstance = axios.create({
            baseURL: this.config.url,
            timeout: this.config.timeout,
            httpAgent: this.httpAgent,
            httpsAgent: this.httpsAgent,
            headers: {
                'Content-Type': 'application/json',
                'Connection': 'keep-alive',
                Authorization: `Basic ${Buffer.from(`${this.config.username}:${this.config.password}`).toString('base64')}`,
            },
        });

        this.setupInterceptors();
        if (this.config.enableCache) {
            this.startCacheCleanup();
        }
    }

    private generateCacheKey(operation: string, body: any): string {
        const cacheableOperations = ['select', 'get_by_hash', 'get_by_hashes', 'search_by_value', 'sql', 'describe_table', 'list_tables', 'list_schemas'];
        if (!cacheableOperations.includes(operation.toLowerCase())) {
            return '';
        }
        try {
            const key = JSON.stringify({ operation, body: this.normalizeBody(body) });
            return Buffer.from(key).toString('base64');
        } catch {
            return '';
        }
    }

    private normalizeBody(body: any): any {
        if (typeof body !== 'object' || body === null) {
            return body;
        }
        const normalized: Record<string, any> = {};
        const sortedKeys = Object.keys(body).sort();
        for (const key of sortedKeys) {
            if (typeof (body as any)[key] === 'object' && (body as any)[key] !== null && !Array.isArray((body as any)[key])) {
                normalized[key] = this.normalizeBody((body as any)[key]);
            } else {
                normalized[key] = (body as any)[key];
            }
        }
        return normalized;
    }

    private getCached(cacheKey: string): HarperDBResponse<any> | null {
        if (!cacheKey || !this.config.enableCache) {
            return null;
        }
        const entry = this.queryCache.get(cacheKey);
        if (!entry) {
            return null;
        }
        const now = Date.now();
        if (now - entry.timestamp > entry.ttl) {
            this.queryCache.delete(cacheKey);
            return null;
        }
        return entry.data;
    }

    private setCache(cacheKey: string, data: HarperDBResponse<any>, ttl?: number): void {
        if (!cacheKey || !this.config.enableCache) {
            return;
        }
        if (this.queryCache.size >= (this.config.maxCacheSize || 0)) {
            const firstKey = this.queryCache.keys().next().value as string | undefined;
            if (firstKey) {
                this.queryCache.delete(firstKey);
            }
        }
        const effectiveTTL = ttl || this.config.cacheTTL || 0;
        this.queryCache.set(cacheKey, {
            data,
            timestamp: Date.now(),
            ttl: effectiveTTL,
        });
    }

    public clearCache(): void {
        this.queryCache.clear();
    }

    private startCacheCleanup(): void {
        setInterval(() => {
            const now = Date.now();
            for (const [key, entry] of this.queryCache.entries()) {
                if (now - entry.timestamp > entry.ttl) {
                    this.queryCache.delete(key);
                }
            }
        }, 60000);
    }

    public invalidateTableCache(table: string, schema?: string): void {
        const schemaName = schema || this.config.schema || '';
        for (const key of this.queryCache.keys()) {
            try {
                const decoded = JSON.parse(Buffer.from(key, 'base64').toString());
                const body = decoded.body || {};
                if (body.table === table && (body.schema === schemaName || !body.schema)) {
                    this.queryCache.delete(key);
                }
            } catch {
                // ignore malformed cache keys
            }
        }
    }

    private setupInterceptors(): void {
        this.axiosInstance.interceptors.request.use((config) => config, (error) => Promise.reject(error));
        this.axiosInstance.interceptors.response.use((response) => response, async (error) => {
            const config = error.config as AxiosRequestConfig & { __retryCount?: number };
            if (!config.__retryCount) {
                config.__retryCount = 0;
            }
            if (
                (config.__retryCount as number) < (this.config.maxRetries || 0) &&
                ((error.response?.status ?? 0) >= 500 || error.code === 'ECONNABORTED')
            ) {
                config.__retryCount = (config.__retryCount as number) + 1;
                await new Promise((resolve) => setTimeout(resolve, (this.config.retryDelay || 0) * (config.__retryCount || 1)));
                return this.axiosInstance(config);
            }
            return Promise.reject(error);
        });
    }

    protected async executeQuery<T = any>(operation: string, body: any, options?: QueryOptions): Promise<HarperDBResponse<T>> {
        try {
            const startTime = Date.now();
            const useCache = options?.useCache !== undefined ? options.useCache : (this.config.enableCache || false);
            const cacheKey = useCache ? this.generateCacheKey(operation, body) : '';
            if (cacheKey) {
                const cached = this.getCached(cacheKey);
                if (cached) {
                    return {
                        ...(cached as any),
                        metadata: {
                            executionTime: cached.metadata?.executionTime || 0,
                            cached: true,
                        },
                    } as HarperDBResponse<T>;
                }
            }

            const endpoint = (this.config.apiPath || '') + '/';
            const requestBody = { operation, ...body };
            const response = await this.axiosInstance.post(endpoint, requestBody, {
                timeout: options?.timeout || this.config.timeout,
            });

            const executionTime = Date.now() - startTime;
            let responseData: any;
            if (Array.isArray(response.data)) {
                responseData = response.data;
            } else if (response.data && typeof response.data === 'object') {
                responseData = response.data.data !== undefined ? response.data.data : response.data;
            } else {
                responseData = response.data;
            }

            const result: HarperDBResponse<T> = {
                message: (response.data?.message as string) || 'Success',
                data: responseData as T,
                metadata: {
                    executionTime,
                },
            };

            if (cacheKey && useCache) {
                const cacheTTL = options?.cacheTTL || this.config.cacheTTL;
                this.setCache(cacheKey, result, cacheTTL);
            }
            return result;
        } catch (error: any) {
            const errorMessage = error.response?.data?.error || error.response?.data?.message || error.message || 'Unknown error';
            throw new Error(`HarperDB query failed: ${errorMessage}`);
        }
    }

    protected async executeParallelQueries<T = any>(
        queries: Array<{ operation: string; body: any; options?: QueryOptions }>,
        concurrency: number = this.maxConcurrentRequests,
    ): Promise<Array<HarperDBResponse<T>>> {
        const results: Array<HarperDBResponse<T>> = [];
        const errors: Error[] = [];
        for (let i = 0; i < queries.length; i += concurrency) {
            const batch = queries.slice(i, i + concurrency);
            const batchPromises = batch.map((query) =>
                this.executeQuery<T>(query.operation, query.body, query.options).catch((error) => {
                    errors.push(error);
                    return null as any;
                }),
            );
            const batchResults = await Promise.all(batchPromises);
            results.push(...(batchResults.filter((r) => r !== null) as Array<HarperDBResponse<T>>));
        }
        return results;
    }

    public getSchema(): string {
        return this.config.schema || '';
    }

    public setSchema(schema: string): void {
        this.config.schema = schema;
    }

    public getConfig(): Readonly<HarperDBConfig> {
        return { ...(this.config as any) } as Readonly<HarperDBConfig>;
    }
}


