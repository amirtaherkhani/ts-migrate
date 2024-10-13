import mysql, { RowDataPacket } from 'mysql2/promise';
import dotenv from 'dotenv';
import { createObjectCsvWriter } from 'csv-writer';
import fs from 'fs';
import path from 'path';
import winston from 'winston';
import chalk from 'chalk';

dotenv.config();

// Set the root of the project as the working directory
const projectRoot = path.resolve(__dirname, '..'); // Assuming your script is inside a subdirectory
process.chdir(projectRoot); // Set the project root as the working directory

// Configure logging
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

// Default list of tables to ignore during export
const defaultIgnoreTables = ['migrations'];

// Fetch additional ignore tables from environment variables or use an empty array if not provided
const additionalIgnoreTables = process.env.IGNORE_TABLES
  ? process.env.IGNORE_TABLES.split(',').map((table) => table.trim())
  : [];

// Merge default and additional ignore tables
const ignoreTables = [...new Set([...defaultIgnoreTables, ...additionalIgnoreTables])];

async function exportDatabaseTables() {
  logger.info('Starting database export process');

  // Use a connection pool
  const pool = mysql.createPool({
    host: process.env.MYSQL_DB_HOST,
    user: process.env.MYSQL_DB_USER,
    password: process.env.MYSQL_DB_PASS,
    database: process.env.MYSQL_DB_NAME,
    port: Number(process.env.MYSQL_DB_PORT),
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0,
  });

  try {
    // Check if connection is successful
    await pool.getConnection().then((conn) => {
      logger.info('Successfully connected to the database');
      conn.release();
    });

    const databaseName = process.env.MYSQL_DB_NAME as string;

    const [tables] = await pool.query<RowDataPacket[]>(
      `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?`,
      [databaseName]
    );

    if (tables.length === 0) {
      logger.warn('No tables found in the database');
      return;
    }

    logger.info(`Found ${tables.length} tables in the database`);

    // Set the output directory to root of the project
    const outputDir = path.resolve(projectRoot, 'exported_data');

    // Check if directory exists, if not, create it
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
      logger.info(`Created output directory: ${outputDir}`);
    }

    // Clean up old exported files if they exist
    if (fs.existsSync(outputDir)) {
      fs.readdirSync(outputDir).forEach((file) => {
        fs.unlinkSync(path.join(outputDir, file));
      });
      logger.info('Old export files removed');
    }

    // Process tables with controlled concurrency
    const concurrencyLimit = 5; // Adjust concurrency limit as needed
    await processTablesWithConcurrencyLimit(tables, concurrencyLimit, pool, databaseName);

    logger.info('Database export process completed successfully');
  } catch (error: any) {
    logger.error(`An error occurred during the export process: ${error.message}`);
  } finally {
    await pool.end();
    logger.info('Database pool closed');
  }
}

async function processTablesWithConcurrencyLimit(
  tables: RowDataPacket[],
  concurrencyLimit: number,
  pool: mysql.Pool,
  databaseName: string
) {
  return new Promise<void>((resolve, reject) => {
    const queue: Promise<void>[] = [];
    let activeCount = 0;
    let index = 0;

    const next = () => {
      while (activeCount < concurrencyLimit && index < tables.length) {
        const row = tables[index++];
        const tableName = row.TABLE_NAME as string;

        // Skip tables that are in the ignore list
        if (ignoreTables.includes(tableName)) {
          logger.info(`Skipping table: ${tableName}`);
          continue;
        }

        activeCount++;
        const promise = exportTableData(pool, tableName, databaseName)
          .then(() => {
            activeCount--;
            next();
          })
          .catch((error) => {
            activeCount--;
            logger.error(`Error exporting table ${tableName}: ${error.message}`);
            next();
          });
        queue.push(promise);
      }

      if (index >= tables.length && activeCount === 0) {
        Promise.all(queue)
          .then(() => resolve())
          .catch((err) => reject(err));
      }
    };

    next();
  });
}

async function exportTableData(
  pool: mysql.Pool,
  tableName: string,
  databaseName: string
) {
  logger.info(`Processing table: ${tableName}`);

  // Get a connection from the pool
  const connection = await pool.getConnection();

  try {
    // Fetch column names for the table
    const [columns] = await connection.query<RowDataPacket[]>(
      `SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
      [databaseName, tableName]
    );

    const columnNames = columns.map((col) => col.COLUMN_NAME as string);

    const outputDir = path.resolve(projectRoot, 'exported_data');
    const fileNameCsv = path.join(outputDir, `${tableName}.csv`);
    const fileNameJson = path.join(outputDir, `${tableName}.json`);

    logger.info(`Starting data export for table: ${tableName}`);

    // Create empty JSON and CSV files if the table is empty
    const [rows] = await connection.query<RowDataPacket[]>(`SELECT * FROM \`${tableName}\` LIMIT 1`);

    if (rows.length === 0) {
      // Efficient creation of empty JSON and CSV files
      fs.writeFileSync(fileNameCsv, ''); // Create empty CSV file
      fs.writeFileSync(fileNameJson, '[]'); // Create empty JSON array
      logger.warn(`Table ${tableName} is empty. Created empty CSV and JSON files.`);
      return;
    }

    // Create write stream for JSON
    const jsonStream = fs.createWriteStream(fileNameJson);
    jsonStream.write('[');

    let isFirstChunk = true;
    let rowCount = 0;
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      const [batchRows] = await connection.query<RowDataPacket[]>(
        `SELECT * FROM \`${tableName}\` LIMIT ? OFFSET ?`,
        [batchSize, offset]
      );

      if (batchRows.length === 0) {
        hasMore = false;
        if (rowCount === 0) {
          logger.warn(`Table ${tableName} is empty`);
        }
        break;
      }

      // Write to CSV
      if (offset === 0 && rowCount === 0) {
        // First batch, overwrite file
        const csvWriter = createObjectCsvWriter({
          path: fileNameCsv,
          header: columnNames.map((col) => ({ id: col, title: col })),
          append: false,
        });
        await csvWriter.writeRecords(batchRows);
      } else {
        // Subsequent batches, append to file
        const csvWriterAppend = createObjectCsvWriter({
          path: fileNameCsv,
          header: columnNames.map((col) => ({ id: col, title: col })),
          append: true,
        });
        await csvWriterAppend.writeRecords(batchRows);
      }

      // Write to JSON
      for (const row of batchRows) {
        if (!isFirstChunk) {
          jsonStream.write(',\n');
        } else {
          isFirstChunk = false;
        }
        jsonStream.write(JSON.stringify(row));
        rowCount++;
        if (rowCount % 1000 === 0) {
          logger.info(`[${tableName}] Processed ${rowCount} rows`);
        }
      }

      offset += batchSize;
    }

    jsonStream.write(']');
    jsonStream.end();

    logger.info(`[${tableName}] Completed data export (Total rows: ${rowCount})`);
  } catch (error: any) {
    logger.error(`An error occurred while exporting table ${tableName}: ${error.message}`);
    throw error;
  } finally {
    // Release the connection back to the pool
    connection.release();
    logger.info(`Released connection for table: ${tableName}`);
  }
}

// Run the export
exportDatabaseTables().catch((error: any) => {
  logger.error(`Unhandled error: ${error.message}`);
});
