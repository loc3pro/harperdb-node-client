/**
 * SQL Migration/Mapping Tool - Similar to Prisma
 * Parse SQL files and create tables automatically
 */
import * as fs from 'fs';
import * as path from 'path';
import { HarperDB } from './harperdb';

export default class HarperDBMigration {
    private db: HarperDB;

    constructor(db: HarperDB) {
        this.db = db;
    }

    parseCreateTable(sql: string): { schema: string; tableName: string; hashAttribute: string; columns: Array<{ name: string; type: string; isPrimaryKey: boolean }> } {
        const createTableRegex = /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:`?(\w+)`?\.)?`?(\w+)`?\s*\(([^)]+)\)/i;
        const match = sql.match(createTableRegex);
        if (!match) {
            throw new Error('Invalid CREATE TABLE statement');
        }
        const schema = match[1] || this.db.getSchema();
        const tableName = match[2];
        const columnsPart = match[3];
        const columns: Array<{ name: string; type: string; isPrimaryKey: boolean }> = [];
        const columnRegex = /`?(\w+)`?\s+(\w+)(?:\([^)]+\))?(?:\s+(?:PRIMARY\s+KEY|AUTO_INCREMENT|NOT\s+NULL|NULL|DEFAULT\s+[^\s,]+))*/gi;
        let columnMatch: RegExpExecArray | null;
        let hashAttribute = 'id';
        while ((columnMatch = columnRegex.exec(columnsPart)) !== null) {
            const columnName = columnMatch[1];
            const columnType = columnMatch[2];
            const primaryKeyMatch = columnsPart.substring(columnMatch.index).match(/PRIMARY\s+KEY/i);
            if (primaryKeyMatch) {
                hashAttribute = columnName;
            }
            columns.push({ name: columnName, type: columnType.toUpperCase(), isPrimaryKey: !!primaryKeyMatch });
        }
        return { schema, tableName, hashAttribute, columns };
    }

    async migrateFromSQL(sqlFilePath: string, options: { force?: boolean } = {}): Promise<any[]> {
        const sql = fs.readFileSync(sqlFilePath, 'utf-8');
        return this.migrateFromSQLString(sql, options);
    }

    async migrateFromSQLString(sql: string, options: { force?: boolean } = {}): Promise<any[]> {
        const results: any[] = [];
        const statements = this.splitSQLStatements(sql);
        for (const statement of statements) {
            const trimmed = statement.trim();
            if (!trimmed) continue;
            if (trimmed.toUpperCase().startsWith('CREATE TABLE')) {
                try {
                    const parsed = this.parseCreateTable(trimmed);
                    const schemaExists = await this.db.schemaExists(parsed.schema);
                    if (!schemaExists) {
                        await this.db.createSchema(parsed.schema);
                        results.push({ type: 'schema_created', schema: parsed.schema });
                    }
                    const tableExists = await this.db.tableExists(parsed.tableName, parsed.schema);
                    if (tableExists && !options.force) {
                        results.push({ type: 'table_skipped', table: parsed.tableName, reason: 'Table already exists' });
                        continue;
                    }
                    if (tableExists && options.force) {
                        await this.db.dropTable(parsed.tableName);
                        results.push({ type: 'table_dropped', table: parsed.tableName });
                    }
                    await this.db.createTable(parsed.tableName, parsed.hashAttribute);
                    results.push({ type: 'table_created', table: parsed.tableName, hashAttribute: parsed.hashAttribute, columns: parsed.columns.length });
                    for (const column of parsed.columns) {
                        if (column.name !== parsed.hashAttribute) {
                            try { await this.db.addAttribute(parsed.tableName, column.name, parsed.schema); } catch {}
                        }
                    }
                } catch (error: any) {
                    results.push({ type: 'error', statement: trimmed.substring(0, 50), error: error.message });
                }
            } else if (trimmed.toUpperCase().startsWith('INSERT INTO')) {
                try {
                    const insertResult = this.parseInsertStatement(trimmed);
                    if (insertResult) {
                        await this.db.insertMany(insertResult.table, insertResult.values as any, { schema: insertResult.schema } as any);
                        results.push({ type: 'data_inserted', table: insertResult.table, count: insertResult.values.length });
                    }
                } catch (error: any) {
                    results.push({ type: 'error', statement: 'INSERT', error: error.message });
                }
            }
        }
        return results;
    }

    parseInsertStatement(sql: string): { schema: string; table: string; values: any[] } | null {
        const insertRegex = /INSERT\s+INTO\s+(?:`?(\w+)`?\.)?`?(\w+)`?\s*\(([^)]+)\)\s*VALUES\s*(.+)/i;
        const match = sql.match(insertRegex);
        if (!match) return null;
        const schema = match[1] || this.db.getSchema();
        const tableName = match[2];
        const columns = match[3].split(',').map(c => c.trim().replace(/`/g, ''));
        const valuesPart = match[4];
        const valueRegex = /\(([^)]+)\)/;
        const valueMatch = valuesPart.match(valueRegex);
        if (!valueMatch) return null;
        const values = valueMatch[1].split(',').map(v => {
            const trimmed = v.trim();
            if ((trimmed.startsWith("'") && trimmed.endsWith("'")) || (trimmed.startsWith('"') && trimmed.endsWith('"'))) {
                return trimmed.slice(1, -1);
            }
            if (!isNaN(Number(trimmed))) {
                return Number(trimmed);
            }
            return trimmed;
        });
        const record: Record<string, any> = {};
        columns.forEach((col, index) => { (record as any)[col] = values[index]; });
        return { schema, table: tableName, values: [record] };
    }

    splitSQLStatements(sql: string): string[] {
        sql = sql.replace(/--.*$/gm, '');
        sql = sql.replace(/\/\*[\s\S]*?\*\//g, '');
        return sql.split(';').filter(s => s.trim().length > 0);
    }

    async generateTypes(tableName: string, schema: string, outputPath?: string): Promise<{ written: boolean; path?: string; types?: string }> {
        const desc = await this.db.describeTable(tableName, { schema } as any);
        const attributes = await this.db.listAttributes(tableName, schema);
        const typeName = this.toPascalCase(tableName);
        let types = `// Auto-generated types for ${schema}.${tableName}\n\n`;
        types += `export interface ${typeName} {\n`;
        if (Array.isArray(attributes.data)) {
            (attributes.data as any).forEach((attr: string) => { types += `  ${attr}: any;\n`; });
        }
        types += `}\n\n`;
        if (outputPath) {
            fs.writeFileSync(outputPath, types);
            return { written: true, path: outputPath };
        }
        return { written: false, types };
    }

    toPascalCase(str: string): string {
        return str.split(/[-_\s]/).map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase()).join('');
    }
}


