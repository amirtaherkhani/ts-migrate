import { Client } from 'pg';
import dotenv from 'dotenv';

// Load environment variables from .env file
dotenv.config();

// Database connection configuration
const client = new Client({
    host: process.env.PG_BR_HOST,
    user: process.env.PG_BR_USER,
    database: process.env.PG_BR_DB_NAME,
    password: process.env.PG_BR_PASS,
    port: parseInt(process.env.PG_BR_PORT || '5432', 10),
    ssl: { rejectUnauthorized: false } // Force SSL and allow self-signed certificates
});

// Test the database connection
async function testConnection() {
    try {
        await client.connect();
        console.log('Connected to PostgreSQL successfully!');
    } catch (err) {
        console.error('Connection failed:', err);
    } finally {
        await client.end();
    }
}

testConnection();
