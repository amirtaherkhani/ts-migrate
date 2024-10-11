import { Client } from 'pg';
import winston from 'winston';
import chalk from 'chalk';

// Setup logger using Winston
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
    transports: [new winston.transports.Console()],
});

// Define source and target database connection details
const sourceConfig = {
    user: 'source_user',
    host: 'source_host',
    database: 'source_db',
    password: 'source_password',
    port: 5432,
};

const targetConfig = {
    user: 'target_user',
    host: 'target_host',
    database: 'target_db',
    password: 'target_password',
    port: 5432,
};

// Utility function to copy data
async function copyTableData(sourceClient: Client, targetClient: Client, tableName: string) {
    logger.info(`Copying data from table: ${tableName}`);

    const copyQuery = `COPY ${tableName} TO STDOUT WITH CSV`;
    const copyInQuery = `COPY ${tableName} FROM STDIN WITH CSV`;

    const sourceStream = sourceClient.query(copyQuery);
    const targetStream = targetClient.query(copyInQuery);

    sourceStream.on('data', (row) => {
        targetStream.write(row);
    });

    sourceStream.on('end', () => {
        targetStream.end();
    });

    sourceStream.on('error', (err) => {
        logger.error(`Error copying data from source for table ${tableName}: ${err}`);
    });

    targetStream.on('error', (err) => {
        logger.error(`Error copying data into target for table ${tableName}: ${err}`);
    });

    await Promise.all([sourceStream, targetStream]);
    logger.info(`Data copied successfully for table: ${tableName}`);
}

// Function to migrate a single table schema and data
async function migrateTable(sourceClient: Client, targetClient: Client, tableName: string) {
    logger.info(`Migrating table: ${tableName}`);

    // Step 1: Copy schema (without data)
    const schemaQuery = `SELECT pg_get_tabledef('${tableName}')`;
    const schemaResult = await sourceClient.query(schemaQuery);
    const createTableQuery = schemaResult.rows[0].pg_get_tabledef;

    try {
        await targetClient.query(createTableQuery);
        logger.info(`Schema migrated for table: ${tableName}`);
    } catch (err) {
        logger.error(`Error migrating schema for table ${tableName}: ${err}`);
    }

    // Step 2: Copy data
    await copyTableData(sourceClient, targetClient, tableName);
}

// Main function to handle the migration
async function migratePostgres() {
    const sourceClient = new Client(sourceConfig);
    const targetClient = new Client(targetConfig);

    try {
        // Connect to source and target databases
        await sourceClient.connect();
        await targetClient.connect();

        // Get a list of all tables in the source database
        const tableResult = await sourceClient.query(
            `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'`
        );
        const tables = tableResult.rows.map((row) => row.table_name);

        // Migrate each table
        for (const table of tables) {
            await migrateTable(sourceClient, targetClient, table);
        }

        logger.info('Migration completed successfully.');
    } catch (err) {
        logger.error(`Error during migration: ${err}`);
    } finally {
        // Close connections
        await sourceClient.end();
        await targetClient.end();
    }
}

// Run the migration
migratePostgres().catch((err) => logger.error(err));
