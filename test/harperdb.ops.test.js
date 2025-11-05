const { HarperDB } = require('../dist/harperdb.js');

describe('HarperDB operations (mocked)', () => {
  let db;
  beforeEach(() => {
    db = new HarperDB({ url: 'https://x', username: 'u', password: 'p', schema: 'dev' });
    db.executeQuery = jest.fn().mockResolvedValue({ message: 'ok', data: [] });
  });

  test('describe_table', async () => {
    await db.describeTable('dogs');
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('describe_table');
    expect(body).toEqual({ schema: 'dev', table: 'dogs' });
  });

  test('list_schemas', async () => {
    await db.listSchemas();
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('list_schemas');
    expect(body).toEqual({});
  });

  test('list_tables', async () => {
    await db.listTables('dev');
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('list_tables');
    expect(body).toEqual({ schema: 'dev' });
  });

  test('describe_all', async () => {
    await db.describeAll();
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('describe_all');
    expect(body).toEqual({});
  });

  test('getByHash', async () => {
    await db.getByHash('dogs', 1);
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('search_by_hash');
    expect(body).toEqual(expect.objectContaining({ table: 'dogs', hash_values: [1] }));
  });

  test('getByHashes', async () => {
    await db.getByHashes('dogs', [1,2]);
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('search_by_hash');
    expect(body).toEqual(expect.objectContaining({ table: 'dogs', hash_values: [1,2] }));
  });

  test('search_by_value', async () => {
    await db.searchByValue('dogs', 'name', 'Fido');
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('search_by_value');
    expect(body).toEqual(expect.objectContaining({ table: 'dogs', search_attribute: 'name', search_value: 'Fido' }));
  });

  test('search_by_conditions', async () => {
    await db.searchByConditions('dogs', [{ name: 'name', operator: '=', value: 'Fido' }]);
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('search_by_conditions');
    expect(body).toEqual(expect.objectContaining({ table: 'dogs' }));
  });

  test('read_transaction_log', async () => {
    await db.readTransactionLog('dogs', { start: 0, end: 1 });
    expect(db.executeQuery).toHaveBeenCalledWith('read_transaction_log', expect.objectContaining({ table: 'dogs', start: 0, end: 1 }), expect.anything());
  });

  test('read_audit_log', async () => {
    await db.readAuditLog('dogs');
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('read_audit_log');
    expect(body).toEqual(expect.objectContaining({ table: 'dogs' }));
  });

  test('delete_transaction_logs_before', async () => {
    await db.deleteTransactionLogsBefore('dogs', 123);
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('delete_transaction_logs_before');
    expect(body).toEqual(expect.objectContaining({ table: 'dogs', timestamp: 123 }));
  });

  test('delete_records_before', async () => {
    await db.deleteRecordsBefore('dogs', '2020-01-01T00:00:00.000Z');
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('delete_records_before');
    expect(body).toEqual(expect.objectContaining({ table: 'dogs', date: '2020-01-01T00:00:00.000Z' }));
  });

  test('get_job', async () => {
    await db.getJob('id');
    const [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('get_job');
    expect(body).toEqual({ job_id: 'id' });
  });

  test('export/import schema', async () => {
    await db.exportSchema({ schema: 'dev' });
    let [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('export_schema');
    expect(body).toEqual({ schema: 'dev' });
    await db.importSchema({ schemas: [] });
    ;[op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('import_schema');
    expect(body).toEqual({ schemas: [] });
  });

  test('backup/restore', async () => {
    await db.backupInstance({ path: '/tmp' });
    expect(db.executeQuery).toHaveBeenCalledWith('backup_instance', { path: '/tmp' }, expect.anything());
    await db.restoreInstance({ path: '/tmp' });
    expect(db.executeQuery).toHaveBeenCalledWith('restore_instance', { path: '/tmp' }, expect.anything());
  });

  test('users', async () => {
    await db.addUser('u2', 'p2', 'user', true);
    let [op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('add_user');
    expect(body).toEqual({ username: 'u2', password: 'p2', role: 'user', active: true });
    await db.alterUser('u2', { active: false });
    ;[op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('alter_user');
    expect(body).toEqual(expect.objectContaining({ username: 'u2', active: false }));
    await db.listUsers();
    ;[op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('list_users');
    expect(body).toEqual({});
    await db.dropUser('u2');
    ;[op, body] = db.executeQuery.mock.calls.pop();
    expect(op).toBe('drop_user');
    expect(body).toEqual({ username: 'u2' });
  });
});


