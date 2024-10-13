import { Client } from 'pg';
import winston from 'winston';
import chalk from 'chalk';

// Setup logger using Winston with colorized output
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp(),
        winston.format.printf(({ timestamp, level, message }) => {
            const colorizedLevel = getColorizedLevel(level);
            return `${timestamp} [${colorizedLevel}] ${message}`;
        })
    ),
    transports: [new winston.transports.Console()],
});

function getColorizedLevel(level: string): string {
    switch (level) {
        case 'info':
            return chalk.blue(level.toUpperCase());
        case 'warn':
            return chalk.yellow(level.toUpperCase());
        case 'error':
            return chalk.red(level.toUpperCase());
        default:
            return level.toUpperCase();
    }
}

// Define source and target database connection details using environment variables for better security
const sourceConfig = {
    user: process.env.SOURCE_DB_USER || 'source_user',
    host: process.env.SOURCE_DB_HOST || 'source_host',
    database: process.env.SOURCE_DB_NAME || 'source_db',
    password: process.env.SOURCE_DB_PASSWORD || 'source_password',
    port: parseInt(process.env.SOURCE_DB_PORT || '5432', 10),
    ssl: { rejectUnauthorized: false }, // Force SSL and allow self-signed certificates
};

const targetConfig = {
    user: process.env.TARGET_DB_USER || 'target_user',
    host: process.env.TARGET_DB_HOST || 'target_host',
    database: process.env.TARGET_DB_NAME || 'target_db',
    password: process.env.TARGET_DB_PASSWORD || 'target_password',
    port: parseInt(process.env.TARGET_DB_PORT || '5432', 10),
    ssl: { rejectUnauthorized: false }, // Force SSL and allow self-signed certificates
};

// Function to check if the target database exists, and create it if it does not
async function ensureDatabaseExists() {
    const adminClient = new Client({
        user: targetConfig.user,
        host: targetConfig.host,
        password: targetConfig.password,
        port: targetConfig.port,
        database: 'postgres', // Connect to the default 'postgres' database or any database that exists
        ssl: { rejectUnauthorized: false }, // Force SSL and allow self-signed certificates
    });

    try {
        await adminClient.connect();

        const dbExistsQuery = `
            SELECT 1 FROM pg_database WHERE datname = '${targetConfig.database}';
        `;

        const result = await adminClient.query(dbExistsQuery);

        if (result.rows.length === 0) {
            logger.info(`Database "${targetConfig.database}" does not exist. Creating...`);

            const createDbQuery = `CREATE DATABASE ${targetConfig.database};`;
            await adminClient.query(createDbQuery);

            logger.info(`Database "${targetConfig.database}" created successfully.`);
        } else {
            logger.info(`Database "${targetConfig.database}" already exists.`);
        }
    } catch (err) {
        logger.error(`Error checking/creating target database: ${err}`);
    } finally {
        await adminClient.end();
    }
}

// Utility function to copy table data from source to target
async function copyTableData(sourceClient: Client, targetClient: Client, tableName: string) {
    logger.info(`Starting data copy for table: ${tableName}`);

    const copyOutQuery = `COPY ${tableName} TO STDOUT WITH CSV`;
    const copyInQuery = `COPY ${tableName} FROM STDIN WITH CSV`;

    const sourceStream = sourceClient.query(copyOutQuery);
    const targetStream = targetClient.query(copyInQuery);

    sourceStream.on('data', (row) => targetStream.write(row));
    sourceStream.on('end', () => targetStream.end());
    sourceStream.on('error', (err) => logger.error(`Error copying data from source: ${err}`));
    targetStream.on('error', (err) => logger.error(`Error writing data to target: ${err}`));

    await Promise.all([sourceStream, targetStream]);

    logger.info(`Data successfully copied for table: ${tableName}`);
}

// Function to migrate schema and data for a single table
async function migrateTable(sourceClient: Client, targetClient: Client, tableName: string) {
    logger.info(`Migrating table: ${tableName}`);

    // Step 1: Copy schema (without data)
    try {
        const schemaQuery = `SELECT pg_get_tabledef('${tableName}')`;
        const schemaResult = await sourceClient.query(schemaQuery);
        const createTableQuery = schemaResult.rows[0].pg_get_tabledef;

        await targetClient.query(createTableQuery);
        logger.info(`Schema successfully migrated for table: ${tableName}`);
    } catch (err) {
        logger.error(`Error migrating schema for table ${tableName}: ${err}`);
    }

    // Step 2: Copy data
    await copyTableData(sourceClient, targetClient, tableName);
}

// Main function to handle the entire migration process
async function migratePostgres() {
    // Ensure target database exists before starting migration
    await ensureDatabaseExists();

    const sourceClient = new Client(sourceConfig);
    const targetClient = new Client(targetConfig);

    try {
        // Connect to both source and target databases
        await sourceClient.connect();
        await targetClient.connect();

        // Retrieve all tables from the source database
        const tableResult = await sourceClient.query(
            `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`
        );
        const tables = tableResult.rows.map((row) => row.table_name);

        // Migrate each table
        for (const table of tables) {
            await migrateTable(sourceClient, targetClient, table);
        }

        logger.info('Database migration completed successfully.');
    } catch (err) {
        logger.error(`Migration failed: ${err}`);
    } finally {
        // Ensure both connections are closed
        await sourceClient.end();
        await targetClient.end();
    }
}

// Run the migration process
migratePostgres().catch((err) => logger.error(`Unexpected error: ${err}`));
