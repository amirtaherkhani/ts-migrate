# 🚀 Database Export and Import Script

This project is a Node.js-based script to export data from a MySQL database into CSV and JSON formats, as well as to import data into a PostgreSQL database. The script is designed to handle multiple tables with controlled concurrency, ignore specific tables, and manage logging using Winston and Chalk for better user experience.

## ✨ Features

- 🔄 Provides a simple and effective TypeScript migration script (`ts-migrate`) for migrating from MySQL to PostgreSQL using TypeORM.
- 🗂️ Displays the schema of MySQL tables, including column details and record counts, with colorized output.
- 📤 Exports MySQL database tables into both CSV and JSON formats.
- 📥 Imports data from CSV and JSON files into a PostgreSQL database.
- ⚙️ Supports controlled concurrency for handling multiple tables at once.
- 📊 Configurable logging using Winston and colored output with Chalk.
- 🌐 Environment variable support for flexibility.
- ⏭️ Ability to skip specific tables during export and import.
- 📋 Easy-to-copy code snippets for common commands.

## 📋 Prerequisites

- Node.js (v12+ recommended)
- MySQL database
- PostgreSQL database
- Yarn

## ⚙️ Installation

1. Clone this repository to your local machine:

   ```sh
   git clone https://github.com/your-username/pg_migrator.git
   cd <repository-directory>
   ```

2. Install the required dependencies:

   ```sh
   yarn install
   ```

3. Set up the environment variables by creating a `.env` file in the root directory:

   ```env
   MYSQL_DB_HOST=your_mysql_host
   MYSQL_DB_USER=your_mysql_user
   MYSQL_DB_PASS=your_mysql_password
   MYSQL_DB_NAME=your_mysql_database_name
   MYSQL_PORT=your_mysql_port (default: 3306)
   PG_HOST=your_postgresql_host
   PG_USER=your_postgresql_user
   PG_PASS=your_postgresql_password
   PG_NAME=your_postgresql_database_name
   PG_PORT=your_postgresql_port (default: 5432)
   IGNORE_TABLES=table1,table2   # (Optional) comma-separated list of tables to ignore
   ```

## 🚀 Usage

### View MySQL Schema

To view the schema of MySQL tables, use the following command:

```sh
yarn schema:mysql
```

This command will display the schema of each table in the MySQL database, including column details and record counts.

### Export Data

To run the export script, use the following command:

```sh
yarn data:export
```

### Import Data

To run the import script, use the following command:

```sh
yarn data:import --type <fileType>
```

Replace `<fileType>` with either `json` or `csv` to specify the type of files to import. The script will export/import the data of each table in the specified database to/from the `exported_data` directory.

The script also provides options for customizing SSL connections and rejecting unauthorized SSL certificates when importing data to PostgreSQL. You can specify these options when running the import command:

- **`--ssl <value>`**: Enable or disable SSL for PostgreSQL connection (`true` or `false`). Default is `true`.
- **`--reject-unauthorized <value>`**: Reject unauthorized SSL certificates (`true` or `false`). Default is `true`.

## 📂 Directory Structure

- **exported_data/**: Contains the exported CSV and JSON files.
- **.env**: Contains configuration details such as database credentials.

## ⚙️ Configuration

You can configure the following aspects of the script using environment variables. Below is a reference for the environment variables:

### MySQL Exporter Environment Variables

- **MYSQL_DB_HOST**: The host of your MySQL server.
- **MYSQL_DB_USER**: The username for your MySQL database.
- **MYSQL_PORT**: The port for MySQL (defaults to `3306`).
- **MYSQL_DB_PASS**: The password for your MySQL database.
- **MYSQL_DB_NAME**: The name of the database you want to export.

### PostgreSQL Importer Environment Variables

- **PG_HOST**: The host of your PostgreSQL server.
- **PG_USER**: The username for your PostgreSQL database.
- **PG_PASS**: The password for your PostgreSQL database.
- **PG_NAME**: The name of the database you want to import into.
- **PG_PORT**: The port for PostgreSQL (defaults to `5432`).

### Additional Environment Variables

- **IGNORE_TABLES**: A comma-separated list of tables to be ignored during the export and import process.

## 📊 Logging

The script uses Winston to handle logging:

- **Info**: Standard information about the export/import process.
- **Warning**: Notifications about empty tables or ignored tables.
- **Error**: Issues encountered during the export/import process.

Logging output is enhanced with color using Chalk.

## ⏱️ Concurrency Limit

The script uses a controlled concurrency limit to manage the number of tables being processed at once. You can adjust the concurrency limit inside the script if needed (`concurrencyLimit` variable).

## ⚠️ Error Handling

If an error occurs during the export/import process, it will be logged and the script will attempt to continue with other tables. All errors are logged using Winston.

## 📜 License

This project is licensed under the MIT License.

## 🤝 Contributions

Feel free to contribute to this project by submitting issues or pull requests. Any feedback or suggestions are welcome!

## 📞 Contact

For any questions, please feel free to reach out.
