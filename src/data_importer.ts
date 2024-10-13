// src/data_importer.ts

// ============================
/* Step 1: Import Dependencies */
// ============================

import { Pool as PgPool, PoolClient as PgPoolClient, QueryResult } from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse';
import winston from 'winston';
import { Command } from 'commander';
import mysql from 'mysql2/promise';
import chalk from 'chalk';

// ============================
/* Step 2: Initialize Environment Variables and Logging */
// ============================

dotenv.config();

const logger = winston.createLogger({
  level: 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(({ timestamp, level, message }) => {
      let colorizedLevel = level.toUpperCase();
      switch (level) {
        case 'info':
          colorizedLevel = chalk.blue(level.toUpperCase());
          break;
        case 'warn':
          colorizedLevel = chalk.yellow(level.toUpperCase());
          break;
        case 'error':
          colorizedLevel = chalk.red(level.toUpperCase());
          break;
        default:
          colorizedLevel = level.toUpperCase();
      }
      return `${timestamp} [${colorizedLevel}] ${message}`;
    })
  ),
  transports: [
    new winston.transports.Console(),
  ],
});

// ============================
/* Step 3: Configure PostgreSQL and MySQL Connection Pools */
// ============================

const program = new Command();

program
  .version('1.0.0')
  .description('PostgreSQL Data Importer')
  .option('-t, --type <fileType>', 'Type of files to import (json, csv)', '')
  .option('--no-ssl', 'Disable SSL for PostgreSQL connection', true)
  .option('--reject-unauthorized', 'Reject unauthorized SSL certificates (default: false)', false)
  .parse(process.argv);

const options = program.opts();

logger.info(`SSL options - SSL Disabled: ${options.noSsl}, Reject Unauthorized: ${options.rejectUnauthorized}`);

const pgPoolConfig: any = {
  host: process.env.PG_HOST,
  user: process.env.PG_USER,
  password: process.env.PG_PASS,
  database: process.env.PG_DB_NAME,
  port: Number(process.env.PG_PORT),
};

if (options.noSsl == false) {
  pgPoolConfig.ssl = { rejectUnauthorized: options.rejectUnauthorized };
}

const pgPool = new PgPool(pgPoolConfig);

const mysqlPool = mysql.createPool({
  host: process.env.MYSQL_DB_HOST,
  user: process.env.MYSQL_DB_USER,
  password: process.env.MYSQL_DB_PASS,
  database: process.env.MYSQL_DB_NAME,
  port: Number(process.env.MYSQL_DB_PORT),
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

// Graceful shutdown on SIGINT (Ctrl+C)
process.on('SIGINT', async () => {
  logger.info('Gracefully shutting down...');
  await pgPool.end();
  await mysqlPool.end();
  process.exit(0);
});

// ============================
/* Step 4: Define Export Directory */
// ============================

const exportDir = path.resolve(__dirname, '..', 'exported_data');

// ============================
/* Step 5: Define Command-Line Interface (CLI) */
// ============================

const fileType = options.type;  // This will give you the value of --type
const validTypes = ['json', 'csv'];
if (!validTypes.includes(fileType)) {
  logger.error('Invalid or missing file type specified. Use --type flag with either "json" or "csv". Example: --type json');
  process.exit(1);
}

logger.info(`File type specified: ${fileType}`);

// ============================
/* Step 6: Utility Functions */
// ============================


function isValidJson(value: any): boolean {
  try {
    JSON.parse(value);
    return true;
  } catch {
    return false;
  }
}


// Function to check PostgreSQL connection
async function checkPgConnection(): Promise<void> {
  logger.info('Checking PostgreSQL connection...');
  try {
    const client: PgPoolClient = await pgPool.connect();
    const res: QueryResult = await client.query('SELECT 1');
    if (res.rowCount === 1) {
      logger.info('PostgreSQL connection successful.');
    } else {
      logger.error('PostgreSQL connection failed.');
      process.exit(1);
    }
    client.release();
  } catch (err) {
    if (err instanceof Error) {
      if (err.message.includes('does not support SSL')) {
        logger.error(`PostgreSQL connection error: ${err.message}. The server does not support SSL connections. Please use the --no-ssl flag to disable SSL.`);
      } else if (err.message.includes('self signed certificate') || err.message.includes('certificate')) {
        logger.error(`PostgreSQL connection error: ${err.message}. The server's SSL certificate is not authorized. Please use the --reject-unauthorized flag to accept unauthorized certificates.`);
      } else {
        logger.error(`PostgreSQL connection error: ${err.message}`);
      }
    } else {
      logger.error(`PostgreSQL connection error: ${err}`);
    }
    process.exit(1);
  }
}

// Function to check MySQL connection and retrieve table list
async function checkMySQLConnectionAndGetTables(): Promise<Set<string>> {
  logger.info('Checking MySQL connection and fetching table list...');
  const connection = await mysqlPool.getConnection();

  try {
    await connection.ping();
    const [tables] = await connection.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = ?`,
      [process.env.MYSQL_DB_NAME]
    );

    logger.info('MySQL connection successful, fetched table list.');

    return new Set((tables as any[]).map((row: { table_name: string }) => row.table_name));
  } catch (err) {
    logger.error(`MySQL connection error: ${err instanceof Error ? err.message : err}`);
    throw err;
  } finally {
    connection.release();
  }
}

// Function to get the PostgreSQL table schema (keeping column names exactly as they are)
async function getPgTableSchema(client: PgPoolClient, tableName: string): Promise<{ column: string; type: string }[]> {
  const query = `
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1
    ORDER BY ordinal_position
  `;
  const res: QueryResult = await client.query(query, [tableName]);
  return res.rows.map((row: { column_name: string; data_type: string }) => ({
    column: row.column_name, // Preserve original column name case
    type: row.data_type,
  }));
}

// Function to read CSV headers (preserving case of headers)
function getCsvHeaders(filePath: string): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const headers: string[] = [];
    const parser = parse({ columns: true, skip_empty_lines: true });

    fs.createReadStream(filePath)
      .pipe(parser)
      .on('headers', (hdrs: string[]) => {
        headers.push(...hdrs); // Preserve original case
      })
      .on('end', () => resolve(headers))
      .on('error', (err: Error) => reject(err));
  });
}

// Function to check if a MySQL table is empty
async function isMySQLTableEmpty(connection: mysql.PoolConnection, tableName: string): Promise<boolean> {
  const query = `SELECT COUNT(*) AS count FROM ${tableName}`;
  const [rows] = await connection.query(query);
  const rowCount = parseInt((rows as any)[0].count, 10);
  return rowCount === 0;
}

// Function to check if a file is empty
function isFileEmpty(filePath: string): boolean {
  const stats = fs.statSync(filePath);
  return stats.size === 0; // Check if file size is 0 bytes
}

// ============================
/* Step 7: Foreign Key Dependency Handling */
// ============================

// Function to get foreign key dependencies for tables
async function getForeignKeyDependencies(client: PgPoolClient): Promise<Map<string, Set<string>>> {
  const query = `
    SELECT
      tc.table_name AS table_name,
      kcu.column_name AS column_name,
      ccu.table_name AS foreign_table_name,
      ccu.column_name AS foreign_column_name
    FROM
      information_schema.table_constraints AS tc
      JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
      JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
    WHERE
      constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public';
  `;

  const res = await client.query(query);

  // Create a map of table dependencies: { tableName -> Set of foreign tables it depends on }
  const dependencies = new Map<string, Set<string>>();
  res.rows.forEach((row: { table_name: string, foreign_table_name: string }) => {
    if (!dependencies.has(row.table_name)) {
      dependencies.set(row.table_name, new Set());
    }
    dependencies.get(row.table_name)?.add(row.foreign_table_name);
  });

  // Log all table dependencies for debugging
  dependencies.forEach((deps, tableName) => {
    logger.debug(`Table "${tableName}" depends on: ${Array.from(deps).join(', ')}`);
  });

  return dependencies;
}

// Function to topologically sort tables based on foreign key dependencies
function topologicalSort(tables: string[], dependencies: Map<string, Set<string>>): string[] {
  const sorted: string[] = [];
  const visited = new Set<string>();
  const visiting = new Set<string>();

  function visit(table: string) {
    if (visited.has(table)) {
      return;
    }

    if (visiting.has(table)) {
      throw new Error(`Cyclic dependency detected in table: ${table}`);
    }

    visiting.add(table);

    const deps = dependencies.get(table) || new Set();
    deps.forEach((dep) => {
      logger.debug(`Visiting dependency "${dep}" for table "${table}"`);
      visit(dep);
    });

    visiting.delete(table);
    visited.add(table);
    sorted.push(table);
  }

  tables.forEach((table) => visit(table));
  logger.debug(`Topologically sorted tables: ${sorted.join(', ')}`);
  return sorted;
}

// ============================
/* Step 8: Schema Validation and Data Import */
// ============================


async function validateAndImportSchemas(tables: string[], mysqlTables: Set<string>): Promise<void> {
  logger.info('Starting schema validation and data import process for MySQL tables and files');

  const pgClient: PgPoolClient = await pgPool.connect();
  const mysqlConnection = await mysqlPool.getConnection();

  try {
    for (const tableName of tables) {
      logger.info(`Validating and importing table: ${tableName}`);

      let exportedColumns: string[] = [];
      const jsonFilePath = path.join(exportDir, `${tableName}.json`);
      const csvFilePath = path.join(exportDir, `${tableName}.csv`);

      const isJsonFileEmpty = fs.existsSync(jsonFilePath) && isFileEmpty(jsonFilePath);
      const isCsvFileEmpty = fs.existsSync(csvFilePath) && isFileEmpty(csvFilePath);

      // Check if the MySQL table exists and is empty
      if (mysqlTables.has(tableName)) {
        const tableIsEmpty = await isMySQLTableEmpty(mysqlConnection, tableName);

        // If both the MySQL table and the file are empty, skip the import
        if (tableIsEmpty && isJsonFileEmpty && isCsvFileEmpty) {
          logger.warn(`Both MySQL table and file for "${tableName}" are empty. Skipping validation and import.`);
          continue;  // Skip import
        }

        // If MySQL table is NOT empty, but the file is empty, stop the process
        if (!tableIsEmpty && (isJsonFileEmpty && isCsvFileEmpty)) {
          logger.error(`File for table "${tableName}" is empty but MySQL table is not empty. Stopping the import process.`);
          throw new Error(`Empty file detected for non-empty MySQL table: ${tableName}`);
        }
      }

      // If both file types are missing (not just empty), skip the import
      if (!fs.existsSync(jsonFilePath) && !fs.existsSync(csvFilePath)) {
        logger.warn(`No files found for table "${tableName}". Skipping import.`);
        continue;  // Skip if no files exist for this table
      }

      // If either the file exists, proceed with schema validation
      const pgSchema: { column: string; type: string }[] = await getPgTableSchema(pgClient, tableName);
      const pgColumns = pgSchema.map(col => col.column);  // Preserve case sensitivity of PostgreSQL columns

      logger.info(`PostgreSQL columns for table ${tableName}: ${pgColumns.join(', ')}`);

      // Fetch exported columns based on the file type (JSON or CSV)
      if (fileType === 'json' && fs.existsSync(jsonFilePath)) {
        const data = fs.readFileSync(jsonFilePath, 'utf-8');
        const jsonData = JSON.parse(data);
        exportedColumns = Object.keys(jsonData[0] || []); // Use exact case from JSON
      } else if (fileType === 'csv' && fs.existsSync(csvFilePath)) {
        const headers = await getCsvHeaders(csvFilePath);
        exportedColumns = headers; // Use exact case from CSV headers
      }

      // If we have no exported columns from the file, skip validation (since no data exists)
      if (exportedColumns.length === 0) {
        logger.warn(`No columns found in the file for "${tableName}". Skipping import.`);
        continue;  // Skip this table as it has no data
      }

      logger.info(`Exported columns for table ${tableName}: ${exportedColumns.join(', ')}`);

      // Compare PostgreSQL and exported columns with case-sensitive matching
      const missingInPg = exportedColumns.filter((col) => !pgColumns.includes(col));
      if (missingInPg.length > 0) {
        logger.error(`Missing columns in PostgreSQL table ${tableName}: ${missingInPg.join(', ')}`);
        throw new Error(`Missing columns in PostgreSQL table: ${tableName}`);
      }

      const extraInPg = pgColumns.filter((col) => !exportedColumns.includes(col));
      if (extraInPg.length > 0) {
        logger.error(`Extra columns in PostgreSQL table ${tableName}: ${extraInPg.join(', ')}`);
        throw new Error(`Extra columns in PostgreSQL table: ${tableName}`);
      }

      const dataFilePath = fileType === 'json' ? jsonFilePath : csvFilePath;

      // Import data with case-sensitive PostgreSQL columns
      await importData(pgClient, tableName, pgSchema.map(col => col.column), dataFilePath);

      logger.info(`Schema validation and data import passed for table: ${tableName}`);
    }
  } finally {
    pgClient.release();
    mysqlConnection.release();
  }
}

// ============================
/* Step 9: Data Import Function */
// ============================

async function importData(pgClient: PgPoolClient, tableName: string, columns: string[], filePath: string): Promise<void> {
  if (!fs.existsSync(filePath)) {
    logger.warn(`File for table "${tableName}" does not exist. Skipping import.`);
    return;
  }

  let data: any[];
  try {
    if (filePath.endsWith('.json')) {
      const fileContent = fs.readFileSync(filePath, 'utf-8');
      data = JSON.parse(fileContent);
    } else if (filePath.endsWith('.csv')) {
      data = [];
      const parser = fs.createReadStream(filePath).pipe(parse({ columns: true }));
      for await (const record of parser) {
        data.push(record);
      }
    } else {
      throw new Error(`Unsupported file type for table "${tableName}".`);
    }
  } catch (err) {
    logger.error(`Failed to parse file for table "${tableName}": ${err instanceof Error ? err.message : err}`);
    return;
  }

  if (data.length === 0) {
    logger.warn(`No data found in file "${filePath}" for table "${tableName}". Skipping import.`);
    return;
  }

  const BATCH_SIZE = 1000;
  const columnList = columns.map((col) => `"${col}"`).join(', ');

  for (let i = 0; i < data.length; i += BATCH_SIZE) {
    const batch = data.slice(i, i + BATCH_SIZE);
    const valuesList = batch
      .map((row) =>
        `(${columns
          .map((col) => {
            let value = row[col];
            if (value === undefined || value === null) {
              return 'NULL'; // Handle NULL values
            }
            if (typeof value === 'object') {
              // If the value is an object, it's likely JSON, so stringify it
              return `'${JSON.stringify(value)}'`;
            } else if (typeof value === 'string' && value.trim().startsWith('{') && value.trim().endsWith('}')) {
              // Check if the string is already a JSON string
              try {
                JSON.parse(value);
                return `'${value}'`;
              } catch {
                return `'${value.replace(/'/g, "''")}'`; // Escape single quotes
              }
            } else {
              // Handle regular string and numeric values, escaping single quotes
              return `'${value.toString().replace(/'/g, "''")}'`;
            }
          })
          .join(', ')})`
      )
      .join(', ');

    const insertQuery = `
      INSERT INTO "${tableName}" (${columnList})
      VALUES ${valuesList}
      ON CONFLICT DO NOTHING;
    `;

    try {
      await pgClient.query(insertQuery);
      logger.info(`Inserted batch ${i / BATCH_SIZE + 1} into table "${tableName}".`);
    } catch (err) {
      logger.error(`Failed to insert batch into table "${tableName}": ${err instanceof Error ? err.message : err}`);
      throw err;
    }
  }
  logger.info(`Data imported successfully into table "${tableName}".`);
}

// ============================
/* Step 10: Main Function */
// ============================

async function main(): Promise<void> {
  logger.info('Starting data import process');

  if (!fs.existsSync(exportDir)) {
    logger.error(`Export directory does not exist: ${exportDir}`);
    process.exit(1);
  }

  try {
    await checkPgConnection();
  } catch (err) {
    logger.error(`PostgreSQL connection check failed: ${err instanceof Error ? err.message : err}`);
    process.exit(1);
  }

  let mysqlTables: Set<string>;
  try {
    mysqlTables = await checkMySQLConnectionAndGetTables();
  } catch (err) {
    logger.error(`MySQL connection check failed: ${err instanceof Error ? err.message : err}`);
    process.exit(1);
  }

  const files: string[] = fs.readdirSync(exportDir).filter((file) => {
    const ext = path.extname(file).toLowerCase();
    return (options.type === 'json' && ext === '.json') || (options.type === 'csv' && ext === '.csv');
  });

  if (files.length === 0) {
    logger.error(`No ${options.type.toUpperCase()} files found in export directory: ${exportDir}`);
    process.exit(1);
  }

  const tablesSet: Set<string> = new Set<string>();
  files.forEach((file) => {
    const ext = path.extname(file).toLowerCase();
    const tableName = path.basename(file, ext);
    tablesSet.add(tableName);
  });

  const tables = Array.from(tablesSet);

  // Fetch foreign key dependencies and perform topological sort
  let sortedTables: string[];
  try {
    const pgClient: PgPoolClient = await pgPool.connect();
    const dependencies = await getForeignKeyDependencies(pgClient); // Fetch foreign key dependencies
    logger.debug('Foreign Key Dependencies:');
    sortedTables = topologicalSort(tables, dependencies); // Sort tables based on dependencies
    logger.info(`Sorted tables for import: ${sortedTables.join(', ')}`);
    pgClient.release();
  } catch (err) {
    logger.error(`Failed to sort tables: ${err instanceof Error ? err.message : err}`);
    process.exit(1);
  }

  // Import the sorted tables
  try {
    await validateAndImportSchemas(sortedTables, mysqlTables); // Use sorted tables for import
  } catch (err) {
    logger.error(`Schema validation failed: ${err instanceof Error ? err.message : err}`);
    process.exit(1);
  }

  logger.info('Data import process completed');

  // Close all connections before exiting
  await pgPool.end();
  await mysqlPool.end();

  // Exit the process cleanly
  process.exit(0);
}

// ============================
/* Step 11: Execute the Main Function */
// ============================

main().catch((err: any) => {
  logger.error(`Unhandled error in import process: ${err instanceof Error ? err.message : err}`);
  process.exit(1);
});