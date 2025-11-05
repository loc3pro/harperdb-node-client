/* Smoke test for newly added operations without hitting network */
const { HarperDB } = require('../dist/harperdb.js');

async function run() {
  const db = new HarperDB({
    url: 'https://example.com',
    username: 'u',
    password: 'p',
    schema: 'dev',
    enableCache: false,
  });

  // Monkey-patch executeQuery to capture payloads
  db.executeQuery = async function (operation, body) {
    console.log('OP:', operation);
    console.log('BODY:', JSON.stringify(body));
    return { message: 'ok', data: [] };
  };

  // Describe/List
  await db.describeTable('dogs');
  await db.describeSchema('dev');
  await db.listSchemas();
  await db.listTables('dev');
  await db.describeAll();

  // Search
  await db.getByHashes('dogs', [1, 2]);
  await db.searchByValue('dogs', 'name', 'Fido', { limit: 10, offset: 0 });
  await db.searchByConditions('dogs', [{ name: 'name', operator: '=', value: 'Fido' }], { operator: 'and' });

  // Logs/Jobs
  await db.readTransactionLog('dogs', { start: 0, end: Date.now() });
  await db.readAuditLog('dogs');
  await db.deleteTransactionLogsBefore('dogs', Date.now());
  await db.deleteRecordsBefore('dogs', new Date().toISOString());
  await db.getJob('00000000-0000-0000-0000-000000000000');

  // System
  await db.systemInformation();

  // Users
  await db.addUser({ username: 'test', password: 'x', role: 'user' });
  await db.alterUser({ username: 'test', active: true });
  await db.listUsers();
  await db.dropUser('test');

  // Export/Backup
  await db.exportSchema({ schema: 'dev' });
  await db.importSchema({ schemas: [] });
  await db.backupInstance({ path: '/tmp/backup' });
  await db.restoreInstance({ path: '/tmp/backup' });

  console.log('Smoke tests executed.');
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});


