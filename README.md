# HarperDB Node.js Client (Optimized)

An optimized, feature-rich HarperDB client for Node.js with parallel queries, bulk operations, and advanced features.

## Features

- ✅ **Parallel Query Execution** - Execute multiple queries simultaneously
- ✅ **Bulk Operations** - Insert, update, upsert, and delete in bulk
- ✅ **Connection Pooling** - Efficient connection management
- ✅ **TCP Keep-Alive** - Reuse TCP connections for better performance
- ✅ **Query Caching** - Cache query results to reduce database load
- ✅ **Automatic Retry** - Built-in retry logic with exponential backoff
- ✅ **TypeScript Support** - Full TypeScript definitions included
- ✅ **Query Builder** - Fluent API for building queries
- ✅ **Transaction-like Operations** - Batch operations with error handling
- ✅ **Advanced CRUD** - Comprehensive CRUD operations
- ✅ **Performance Monitoring** - Built-in execution time tracking
- ✅ **Full HarperDB API Coverage** - All REST API and Operations API methods
- ✅ **Schema & Table Management** - Create, list, drop schemas and tables
- ✅ **User Management** - Add, alter, drop, and list users
- ✅ **Attribute Management** - Add, drop, and list table attributes
- ✅ **Index Management** - Create, drop, list, and check indexes on table attributes
- ✅ **GraphQL Schema Support** - Load and apply GraphQL schema files to automatically create tables
- ✅ **System Operations** - System info, cluster status, logs, and more

## Installation

```bash
npm install harperdb-node-client
```

## Quick Start

```typescript
import HarperDB from 'harperdb-node-client';llll         

const db = new HarperDB({
  url: 'https://your-instance.harperdbcloud.com',
  username: 'your-username',
  password: 'your-password',
  schema: 'dev', // optional, defaults to 'dev'
  timeout: 30000, // optional
  maxRetries: 3, // optional
  poolSize: 10, // optional, concurrent requests
  keepAlive: true, // optional, enable TCP keep-alive (default: true)
  enableCache: true, // optional, enable query caching (default: false)
  cacheTTL: 5000, // optional, cache TTL in milliseconds (default: 5000)
  maxCacheSize: 1000, // optional, max cache entries (default: 1000)
});

// Insert a record
const result = await db.insert('users', {
  id: '1',
  name: 'John Doe',
  email: 'john@example.com'
});

// Select records
const users = await db.select('users', {
  limit: 10,
  where: { name: 'John Doe' }
});

// Update a record
await db.update('users', {
  id: '1',
  name: 'Jane Doe',
  email: 'jane@example.com'
});

// Delete a record
await db.delete('users', '1');
```

## API Reference

### Initialization

```typescript
const db = new HarperDB({
  url: string;           // HarperDB instance URL
  username: string;      // HarperDB username
  password: string;      // HarperDB password
  schema?: string;       // Default schema (default: 'dev')
  timeout?: number;      // Request timeout in ms (default: 30000)
  maxRetries?: number;   // Max retry attempts (default: 3)
  retryDelay?: number;   // Retry delay in ms (default: 1000)
  poolSize?: number;     // Concurrent requests (default: 10)
});
```

### CRUD Operations

#### Insert

```typescript
// Insert single record
await db.insert('users', {
  id: '1',
  name: 'John Doe'
});

// Insert multiple records (bulk)
const result = await db.insertMany('users', [
  { id: '1', name: 'John' },
  { id: '2', name: 'Jane' }
], {
  batchSize: 1000 // optional, default: 1000
});

console.log(result);
// {
//   successful: 2,
//   failed: 0,
//   executionTime: 150
// }
```

#### Update

```typescript
// Update single record
await db.update('users', {
  id: '1',
  name: 'John Updated'
});

// Update multiple records
await db.updateMany('users', [
  { id: '1', name: 'John Updated' },
  { id: '2', name: 'Jane Updated' }
]);
```

#### Upsert

```typescript
// Upsert single record (insert or update)
await db.upsert('users', {
  id: '1',
  name: 'John Doe',
  email: 'john@example.com'
});

// Bulk upsert
const result = await db.upsertMany('users', [
  { id: '1', name: 'John' },
  { id: '2', name: 'Jane' }
], {
  batchSize: 1000
});
```

#### Delete

```typescript
// Delete single record
await db.delete('users', '1'); // hash value

// Delete multiple records
const result = await db.deleteMany('users', ['1', '2', '3'], {
  batchSize: 1000
});
```

#### Select

```typescript
// Select all records
const allUsers = await db.select('users');

// Select with options
const users = await db.select('users', {
  limit: 10,
  offset: 0,
  where: { name: 'John' },
  orderBy: 'name',
  orderDirection: 'asc'
});

// Get by hash value
const user = await db.getByHash('users', '1');

// Search by value
const results = await db.searchByValue(
  'users',
  'name',
  'John',
  ['id', 'name', 'email'] // attributes to return
);
```

### SQL Queries

```typescript
// Execute raw SQL
const results = await db.sql('SELECT * FROM dev.users WHERE name = ?', {
  timeout: 5000
});
```

### Parallel Queries

Execute multiple queries simultaneously:

```typescript
const results = await db.parallel([
  {
    type: 'select',
    table: 'users',
    options: { limit: 10 }
  },
  {
    type: 'select',
    table: 'products',
    options: { limit: 20 }
  },
  {
    type: 'insert',
    table: 'logs',
    data: { message: 'Query executed' }
  }
], {
  concurrency: 5, // optional, default: poolSize
  failFast: false // optional, stop on first error
});

console.log(results);
// {
//   results: [...],
//   executionTime: 250,
//   successful: 3,
//   failed: 0
// }
```

### Query Builder

Fluent API for building queries:

```typescript
const users = await db.query('users')
  .where({ status: 'active' })
  .limit(10)
  .offset(0)
  .orderBy('name', 'asc')
  .execute();
```

### Schema and Table Management

```typescript
// Create schema
await db.createSchema('my_schema');

// Drop schema
await db.dropSchema('my_schema');

// List all schemas
const schemas = await db.listSchemas();

// Check if schema exists
const exists = await db.schemaExists('my_schema');

// Create table
await db.createTable('users', 'id'); // table name, hash attribute

// Drop table
await db.dropTable('users');

// List all tables in a schema
const tables = await db.listTables('my_schema');

// Check if table exists
const tableExists = await db.tableExists('users', 'my_schema');

// Describe table
const schema = await db.describeTable('users');

// Get table schema (alias for describeTable)
const tableSchema = await db.getTableSchema('users');
```

### User Management

```typescript
// Add a new user
await db.addUser('newuser', 'password123', 'user', true);
// Parameters: username, password, role, active

// List all users
const users = await db.listUsers();

// Alter user (update user properties)
await db.alterUser('newuser', {
  password: 'newpassword',
  role: 'read_only',
  active: false
});

// Drop a user
await db.dropUser('newuser');
```

### Attribute Management

```typescript
// List attributes of a table
const attributes = await db.listAttributes('users', 'my_schema');

// Add attribute to a table
await db.addAttribute('users', 'new_field', 'my_schema');

// Drop attribute from a table
await db.dropAttribute('users', 'old_field', 'my_schema');
```

### Index Management

```typescript
// Create index on table attribute
await db.createIndex('products', 'category');
// Creates index on 'category' attribute to improve query performance

// Create unique index
await db.createIndex('users', 'email', {
  unique: true,
  indexName: 'idx_email_unique'
});

// List all indexes on a table
const indexes = await db.listIndexes('products');
// Returns: [{ attribute: 'category', unique: false, name: 'idx_category' }, ...]

// Check if index exists
const exists = await db.indexExists('products', 'category');
// Returns: true/false

// Drop index (removes the indexed attribute)
await db.dropIndex('products', 'category');
// Note: This will remove the attribute entirely, not just the index
```

### GraphQL Schema Support

Load and apply GraphQL schema files to automatically create tables, schemas, and indexes:

```typescript
// Apply schema from file (recommended)
// File will be validated before processing
const result = await db.applySchemaFromFile('schema.graphql', {
  force: false,        // Drop and recreate existing tables
  skipExisting: true  // Skip tables that already exist
});

console.log(result.data);
// {
//   schemasCreated: ['token_ban'],
//   tablesCreated: ['token_ban.Whitelist', 'token_ban.Admin', ...],
//   attributesCreated: ['token_ban.Whitelist.id', 'token_ban.Whitelist.tokenId', ...],
//   indexesCreated: ['token_ban.Whitelist.tokenId', ...],
//   errors: []
// }

// Validate schema file before applying
const validation = db.validateSchemaFile('schema.graphql');
if (!validation.valid) {
  console.error('Invalid schema:', validation.error);
  return;
}

// Parse and apply schema string
const schemaString = `
type User @table(database: "my_schema") @export {
    id: ID @primaryKey @indexed
    username: String! @indexed
    email: String! @indexed
    age: Int
}
`;

const types = db.parseGraphQLSchema(schemaString);
await db.applySchema(types);

// Or apply schema string directly
await db.applySchema(schemaString);
```

**Schema File Validation:**

The package automatically validates schema files before processing:

- ✅ **File exists** - Checks if file path is valid
- ✅ **File extension** - Must be `.graphql` or `.gql`
- ✅ **File content** - Must contain `type` definitions
- ✅ **@table directive** - Must contain `@table` directive for HarperDB
- ✅ **Valid structure** - Must parse successfully and contain valid table types

**Supported GraphQL Directives:**
- `@table(database: "schema_name")` - Specify the schema/database name
- `@table(database: "schema_name", replicate: false)` - Control replication
- `@primaryKey` - Mark field as primary key (hash attribute)
- `@indexed` - Create index on field

**Example Schema File (schema.graphql):**
```graphql
type Whitelist @table(database: "token_ban") @export {
    id: ID @primaryKey @indexed
    tokenId: String @indexed
    path: String
    requestIp: String
    createdBy: String!
    createdOn: Date
}

type Admin @table(database: "token_ban") @export {
    id: ID @primaryKey @indexed
    username: String! @indexed
    password: String!
    role: String!
    status: String!
}
```

### System Operations

```typescript
// Get system information
const sysInfo = await db.systemInformation();

// Get cluster status
const clusterStatus = await db.clusterStatus();

// Get node status
const nodeStatus = await db.nodeStatus();

// Read log entries
const logs = await db.readLog(100, 0); // limit, start offset
```

### Advanced Delete Operations

```typescript
// Delete by hash value (existing)
await db.delete('users', 'hash-value');

// Delete by value (new - more flexible)
const result = await db.deleteByValue('users', 'email', 'user@example.com');
// This deletes all records where email = 'user@example.com'
```

### Advanced Features

#### Custom Hash Attribute

```typescript
await db.insert('users', {
  customId: '123',
  name: 'John'
}, {
  hashAttribute: 'customId'
});
```

#### Error Handling

```typescript
try {
  const result = await db.insert('users', { id: '1', name: 'John' });
} catch (error) {
  console.error('Operation failed:', error.message);
}
```

#### Bulk Operations with Error Tracking

```typescript
const result = await db.insertMany('users', usersArray);

if (result.failed > 0) {
  console.error('Failed inserts:', result.errors);
  // result.errors contains array of { index, error }
}
```

## Performance Tips

### TCP Keep-Alive & Connection Pooling

Enable TCP keep-alive to reuse connections and reduce overhead:

```typescript
const db = new HarperDB({
  url: '...',
  username: '...',
  password: '...',
  keepAlive: true,        // Enable TCP keep-alive (default: true)
  keepAliveMsecs: 1000,   // Keep-alive interval in milliseconds
  maxSockets: 50,         // Max sockets per host
  maxFreeSockets: 10,     // Max free sockets to keep alive
});
```

**Benefits:**
- ✅ Reuses TCP connections instead of creating new ones
- ✅ Reduces connection overhead by ~50-70%
- ✅ Faster subsequent requests
- ✅ Lower latency for multiple queries

### Query Caching

Enable query caching to cache read operations:

```typescript
const db = new HarperDB({
  url: '...',
  username: '...',
  password: '...',
  enableCache: true,      // Enable query caching
  cacheTTL: 5000,         // Cache TTL in milliseconds (5 seconds)
  maxCacheSize: 1000,     // Maximum cache entries
});

// Cache is automatically invalidated on write operations
await db.select('users'); // First call - from database
await db.select('users'); // Second call - from cache (instant!)

// Write operations automatically invalidate cache
await db.insert('users', { id: '1', name: 'John' });
await db.select('users'); // Cache invalidated, fetches fresh data
```

**Cache Management:**

```typescript
// Clear all cache
db.clearCache();

// Invalidate cache for specific table
db.invalidateTableCache('users');

// Override cache for specific query
await db.select('users', { useCache: false });

// Custom cache TTL for specific query
await db.select('users', { cacheTTL: 10000 }); // 10 seconds
```

**Benefits:**
- ✅ Instant responses for cached queries (0-1ms vs 50-100ms)
- ✅ Reduced database load
- ✅ Automatic cache invalidation on writes
- ✅ Configurable TTL and cache size

### Combined Optimizations

For maximum performance, combine both optimizations:

```typescript
const db = new HarperDB({
  url: '...',
  username: '...',
  password: '...',
  // TCP Optimization
  keepAlive: true,
  maxSockets: 50,
  // Query Caching
  enableCache: true,
  cacheTTL: 5000,
  maxCacheSize: 1000,
});
```

**Performance Improvement:**
- **Without optimizations:** ~50-100ms per query
- **With keep-alive:** ~30-50ms per query (40-50% faster)
- **With caching:** ~0-1ms for cached queries (99% faster)
- **Combined:** Best of both worlds!

1. **Use Bulk Operations**: Always prefer `insertMany`, `upsertMany`, `deleteMany` over multiple single operations
2. **Parallel Queries**: Use `parallel()` for independent queries
3. **Connection Pooling**: Adjust `poolSize` based on your workload
4. **Batch Size**: Tune `batchSize` for bulk operations (default: 1000)

## TypeScript Support

Full TypeScript definitions are included:

```typescript
import HarperDB, { QueryResult, BulkOperationResult } from 'harperdb-node-client';

interface User {
  id: string;
  name: string;
  email: string;
}

const db = new HarperDB({...});

const result: QueryResult<User[]> = await db.select<User>('users');
const bulkResult: BulkOperationResult = await db.insertMany<User>('users', users);
```

## Examples

### Example 1: Batch Processing

```typescript
// Process large dataset in batches
const allUsers = await fetchUsersFromAPI();
const result = await db.upsertMany('users', allUsers, {
  batchSize: 500
});

console.log(`Processed ${result.successful} users in ${result.executionTime}ms`);
```

### Example 2: Parallel Data Loading

```typescript
// Load multiple tables in parallel
const [users, products, orders] = await Promise.all([
  db.select('users', { limit: 100 }),
  db.select('products', { limit: 100 }),
  db.select('orders', { limit: 100 })
]);
```

### Example 3: Complex Query

```typescript
// Use query builder for complex queries
const activeUsers = await db.query('users')
  .where({ status: 'active' })
  .limit(50)
  .orderBy('created_at', 'desc')
  .execute();
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
