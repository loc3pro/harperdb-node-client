const { HarperDB } = require('../dist/harperdb.js');

describe('HarperDB core (mocked)', () => {
  let db;
  beforeEach(() => {
    db = new HarperDB({ url: 'https://x', username: 'u', password: 'p', schema: 'dev' });
    db.executeQuery = jest.fn().mockResolvedValue({ message: 'ok', data: [] });
    db.invalidateTableCache = jest.fn();
    db.indexExists = jest.fn().mockResolvedValue(false);
    db.listIndexes = jest.fn().mockResolvedValue({ data: [] });
  });

  test('insert', async () => {
    await db.insert('t', { id: '1', a: 1 });
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('insert');
    expect(body).toEqual(expect.objectContaining({ schema: 'dev', table: 't', records: [expect.any(Object)] }));
    expect(db.invalidateTableCache).toHaveBeenCalled();
  });

  test('update', async () => {
    await db.update('t', { id: '1', a: 2 });
    const [op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('update');
    expect(db.invalidateTableCache).toHaveBeenCalled();
  });

  test('upsert', async () => {
    await db.upsert('t', { id: '1', a: 3 });
    const [op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('upsert');
    expect(db.invalidateTableCache).toHaveBeenCalled();
  });

  test('delete', async () => {
    await db.delete('t', '1');
    const [op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('delete');
    expect(db.invalidateTableCache).toHaveBeenCalled();
  });

  test('insertMany', async () => {
    await db.insertMany('t', [{ id: '1' }, { id: '2' }], { batchSize: 1 });
    const ops = db.executeQuery.mock.calls.map(c => c[0]);
    expect(ops.filter(o => o === 'insert').length).toBeGreaterThan(0);
  });

  test('deleteMany', async () => {
    await db.deleteMany('t', ['1','2']);
    const [op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('delete');
  });

  test('sql passthrough', async () => {
    await db.sql('SELECT 1');
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('sql');
    expect(body).toEqual({ sql: 'SELECT 1' });
  });

  test('query passthrough', async () => {
    await db.query('SELECT * FROM dev.t LIMIT 1');
    const [op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('sql');
  });

  test('create/drop schema', async () => {
    await db.createSchema('s');
    let [op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('create_schema');
    await db.dropSchema('s');
    ;[op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('drop_schema');
  });

  test('create/drop table', async () => {
    await db.createTable('t', 'id');
    let [op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('create_table');
    await db.dropTable('t');
    ;[op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('drop_table');
  });

  test('attributes', async () => {
    await db.addAttribute('t', 'col');
    let [op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('add_attribute');
    await db.dropAttribute('t', 'col');
    ;[op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('drop_attribute');
  });

  test('indexes', async () => {
    await db.createIndex('t', 'col');
    const ops = db.executeQuery.mock.calls.map(c => c[0]);
    expect(ops).toContain('add_attribute');
    db.indexExists = jest.fn().mockResolvedValue(true);
    await db.dropIndex('t', 'col');
    // dropIndex routes to drop_attribute when exists
  });

  test('paginate', async () => {
    await db.paginate('t', { page: 1, pageSize: 10 });
    const [op] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('sql');
  });
});


