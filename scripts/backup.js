const fs = require("fs");
const path = require("path");
require("dotenv").config();
const mysql = require("mysql2/promise");

const getDbConfig = () => {
  const connectionUrl = String(
    process.env.DATABASE_URL ||
      process.env.MYSQL_URL ||
      process.env.JAWSDB_URL ||
      "",
  ).trim();

  if (connectionUrl) {
    const parsedUrl = new URL(connectionUrl);
    return {
      host: parsedUrl.hostname,
      port: parsedUrl.port ? Number(parsedUrl.port) : 3306,
      user: decodeURIComponent(parsedUrl.username || ""),
      password: decodeURIComponent(parsedUrl.password || ""),
      database: decodeURIComponent(
        String(parsedUrl.pathname || "").replace(/^\//, ""),
      ),
      ssl: /aivencloud\.com$/i.test(parsedUrl.hostname)
        ? { rejectUnauthorized: false }
        : undefined,
    };
  }

  return {
    host: process.env.DB_HOST || "localhost",
    port: process.env.DB_PORT ? Number(process.env.DB_PORT) : 3306,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD || "",
    database: process.env.DB_NAME,
    ssl: /aivencloud\.com$/i.test(process.env.DB_HOST || "")
      ? { rejectUnauthorized: false }
      : undefined,
  };
};

async function createBackup() {
  const config = getDbConfig();
  if (!config.database) {
    console.error("Error: DB_NAME environment variable is missing.");
    process.exit(1);
  }

  console.log(`Connecting to database ${config.database} on ${config.host}...`);

  let connection;
  try {
    connection = await mysql.createConnection(config);
  } catch (err) {
    console.error("Failed to connect to database:", err.message);
    process.exit(1);
  }

  try {
    const backupDir = path.join(__dirname, "..", "backups");
    if (!fs.existsSync(backupDir)) {
      fs.mkdirSync(backupDir, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const filename = `backup-${config.database}-${timestamp}.sql`;
    const filePath = path.join(backupDir, filename);

    const writeStream = fs.createWriteStream(filePath, { encoding: "utf8" });

    writeStream.write(`-- Hirenext Database Backup\n`);
    writeStream.write(`-- Database: ${config.database}\n`);
    writeStream.write(`-- Generated at: ${new Date().toISOString()}\n\n`);
    writeStream.write(`SET FOREIGN_KEY_CHECKS=0;\n\n`);

    const [tablesRows] = await connection.query("SHOW TABLES");
    const tableKey = Object.keys(tablesRows[0] || {})[0];
    const tables = tablesRows.map((r) => r[tableKey]);

    for (const table of tables) {
      console.log(`Exporting table: ${table}...`);

      const [createRows] = await connection.query(`SHOW CREATE TABLE \`${table}\``);
      const createSql = createRows[0]["Create Table"] || createRows[0]["Create View"];

      writeStream.write(`-- Table structure for \`${table}\` --\n`);
      writeStream.write(`DROP TABLE IF EXISTS \`${table}\`;\n`);
      writeStream.write(`${createSql};\n\n`);

      const [rows] = await connection.query(`SELECT * FROM \`${table}\``);
      if (rows.length > 0) {
        writeStream.write(`-- Data for \`${table}\` --\n`);
        for (const row of rows) {
          const values = Object.values(row)
            .map((val) => {
              if (val === null || val === undefined) return "NULL";
              if (typeof val === "number") return val;
              if (typeof val === "boolean") return val ? 1 : 0;
              if (val instanceof Date) return `'${val.toISOString().slice(0, 19).replace("T", " ")}'`;
              if (Buffer.isBuffer(val)) return `0x${val.toString("hex")}`;
              if (typeof val === "object") return connection.escape(JSON.stringify(val));
              return connection.escape(String(val));
            })
            .join(", ");
          writeStream.write(`INSERT INTO \`${table}\` VALUES (${values});\n`);
        }
        writeStream.write(`\n`);
      }
    }

    writeStream.write(`SET FOREIGN_KEY_CHECKS=1;\n`);
    writeStream.end();

    await new Promise((resolve) => writeStream.on("finish", resolve));
    console.log(`\nBackup created successfully! Saved to:\n${filePath}`);
  } catch (err) {
    console.error("Error creating backup:", err.message);
  } finally {
    await connection.end();
  }
}

createBackup();
