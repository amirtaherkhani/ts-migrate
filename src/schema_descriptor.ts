import mysql from 'mysql2/promise';
import dotenv from 'dotenv';
const chalk = require('chalk'); // Using chalk@4 for CommonJS compatibility

dotenv.config();

interface ColumnSchema {
  columnName: string;
  dataType: string;
  isNullable: string;
  columnDefault: any;
  columnKey: string;
}

interface TableSchema {
  tableName: string;
  columns: ColumnSchema[];
  rowCount: number; // Added rowCount property
}

async function getDatabaseSchema(): Promise<TableSchema[]> {
  const connection = await mysql.createConnection({
    host: process.env.MYSQL_DB_HOST,
    user: process.env.MYSQL_DB_USER,
    password: process.env.MYSQL_DB_PASS,
    database: process.env.MYSQL_DB_NAME,
    port: Number(process.env.MYSQL_PORT),
  });

  const [tables] = await connection.query(
    `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ?`,
    [connection.config.database]
  );

  const schema: TableSchema[] = [];

  for (const row of tables as any[]) {
    const tableName = row.TABLE_NAME;

    // Get the count of records in the table
    const [countResult] = await connection.query(
      `SELECT COUNT(*) as count FROM \`${tableName}\``
    );
    const rowCount = (countResult as any[])[0].count;

    // Output the table name and record count
    console.log(chalk.blue.bold(`Table: ${tableName}`) + chalk.magenta(` (Records: ${rowCount})`));

    const [columns] = await connection.query(
      `SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT, COLUMN_KEY
       FROM INFORMATION_SCHEMA.COLUMNS
       WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
      [connection.config.database, tableName]
    );

    const columnSchemas: ColumnSchema[] = (columns as any[]).map(column => ({
      columnName: column.COLUMN_NAME,
      dataType: column.DATA_TYPE,
      isNullable: column.IS_NULLABLE,
      columnDefault: column.COLUMN_DEFAULT,
      columnKey: column.COLUMN_KEY,
    }));

    schema.push({
      tableName,
      columns: columnSchemas,
      rowCount, // Store the row count
    });

    // Log column details with colorized output
    for (const column of columnSchemas) {
      console.log(chalk.green(`  Column: ${column.columnName}`));
      console.log(`    ${chalk.yellow('Data Type')}: ${column.dataType}`);
      console.log(`    ${chalk.yellow('Is Nullable')}: ${column.isNullable}`);
      console.log(`    ${chalk.yellow('Default')}: ${column.columnDefault ?? 'None'}`);
      console.log(`    ${chalk.yellow('Key')}: ${column.columnKey}`);

      // Highlight primary keys
      if (column.columnKey === 'PRI') {
        console.log(chalk.red(`    Primary Key`));
      }
    }

    console.log('\n');
  }

  await connection.end();
  return schema;
}

getDatabaseSchema()
  .then(schema => {
    // You can process the schema here if needed
    // For example, write it to a file
    // fs.writeFileSync('schema.json', JSON.stringify(schema, null, 2));
  })
  .catch(console.error);
