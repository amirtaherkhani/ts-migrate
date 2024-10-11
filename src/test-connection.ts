import { Client } from 'pg';
import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

// Database connection configuration using environment variables
const client = new Client({
    host: process.env.PG_BR_HOST,
    user: process.env.PG_BR_USER,
    database: process.env.PG_BR_DB_NAME,
    password: process.env.PG_BR_PASS,
    port: parseInt(process.env.PG_BR_PORT || '5432', 10), // Fallback to default port 5432 if PGPORT is not set
});

// Test database connection
async function testConnection() {
    try {
        await client.connect();
        console.log('Connected to PostgreSQL successfully!');
    } catch (err) {
        if (err instanceof Error) {
            console.error('Connection to PostgreSQL failed:', err.stack);
        } else {
            console.error('Unknown error:', err);
        }
    } finally {
        await client.end();
    }
}

testConnection();
