const fs = require("fs");
const path = require("path");
const { DATA_DIR, DB_PATH, PORT } = require("./config");
const { hasApplicationSchema, listExistingSqliteFiles, openDatabase, resolveRuntimeDbPath } = require("./db");
const { isImportableDatabaseFile } = require("./db-transfer");
const { ensureOwnerMetricsCompatibility, ensureRegionMetricsCompatibility } = require("./seed");
const { createApp } = require("./app");

const runtimeDbPath = resolveRuntimeDbPath();
const runtimeDbExisted = fs.existsSync(runtimeDbPath);
const db = openDatabase();

function findLatestSqliteFile(filePaths) {
  return filePaths
    .map((filePath) => ({
      filePath,
      modifiedAt: fs.statSync(filePath).mtimeMs,
    }))
    .sort((left, right) => right.modifiedAt - left.modifiedAt)[0]?.filePath;
}

function listTransferFiles(directoryPath) {
  if (!fs.existsSync(directoryPath)) {
    return [];
  }
  return fs
    .readdirSync(directoryPath, { withFileTypes: true })
    .filter((entry) => entry.isFile() && isImportableDatabaseFile(entry.name))
    .map((entry) => path.resolve(directoryPath, entry.name));
}

const hasSchema = db
  .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'regions'")
  .get();

if (!hasSchema) {
  const siblingDatabases = listExistingSqliteFiles(DATA_DIR).filter(
    (filePath) => path.resolve(filePath) !== path.resolve(runtimeDbPath)
  );
  const usableSiblingDatabases = siblingDatabases.filter(hasApplicationSchema);
  const exportDir = path.join(DATA_DIR, "exports");
  const exportCandidates = listTransferFiles(exportDir);
  const latestExport = findLatestSqliteFile(exportCandidates);

  console.error(`Audit dashboard schema was not found at ${runtimeDbPath}.`);

  if (!runtimeDbExisted && path.resolve(runtimeDbPath) === path.resolve(DB_PATH)) {
    console.error(`Startup created an empty SQLite file at ${runtimeDbPath} because the configured DB was missing.`);
  }

  if (usableSiblingDatabases.length) {
    console.error(`Found other SQLite files with the expected schema in ${DATA_DIR}:`);
    usableSiblingDatabases.forEach((filePath) => console.error(`- ${filePath}`));
    console.error(`Rename the desired file to ${path.basename(DB_PATH)} or set SQLITE_PATH to point to it.`);
  }

  if (latestExport) {
    console.error(`Database dump files inside ${exportDir} are not loaded automatically.`);
    console.error(`Import the latest export with: npm.cmd run db:import -- --in "${latestExport}"`);
  }

  console.error(`Run "npm.cmd run db:reset" inside backend/ if you want to rebuild the database from seed data.`);
  db.close();
  process.exit(1);
}

if (ensureRegionMetricsCompatibility(db)) {
  console.log("Region metrics schema was outdated. Rebuilt owner-scoped aggregates.");
}

if (ensureOwnerMetricsCompatibility(db)) {
  console.log("Owner metrics table was missing or outdated. Rebuilt national owner aggregates.");
}

db.exec("CREATE INDEX IF NOT EXISTS idx_packages_owner_lookup ON packages(owner_type, owner_name);");

const app = createApp(db);

// Simpan db ke app untuk diakses route
app.set('db', db);
app.locals.db = db;

// ========== REGISTRASI ROUTE BANDUNG ==========
try {
  const bandungRoutesPath = path.join(__dirname, 'routes', 'bandung.js');
  if (fs.existsSync(bandungRoutesPath)) {
    const bandungRoutes = require('./routes/bandung');
    app.use('/api/bandung', bandungRoutes);
    console.log('✅ Bandung Raya API routes registered at /api/bandung');
  } else {
    console.warn('⚠️ Bandung routes file not found at:', bandungRoutesPath);
  }
} catch (error) {
  console.error('❌ Failed to register Bandung routes:', error.message);
}

// ========== REGISTRASI ROUTE UMKM ==========
try {
  const umkmRoutesPath = path.join(__dirname, 'routes', 'umkm.js');
  if (fs.existsSync(umkmRoutesPath)) {
    const umkmRoutes = require('./routes/umkm');
    app.use('/api/umkm', umkmRoutes);
    console.log('✅ UMKM API routes registered at /api/umkm');
  } else {
    console.warn('⚠️ UMKM routes file not found at:', umkmRoutesPath);
  }
} catch (error) {
  console.error('❌ Failed to register UMKM routes:', error.message);
}

// ========== API ENDPOINTS TAMBAHAN ==========
app.get('/api/health', (req, res) => {
  res.json({ 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    database: runtimeDbPath,
    dbSize: fs.existsSync(runtimeDbPath) ? fs.statSync(runtimeDbPath).size : 0
  });
});

app.get('/api/stats', (req, res) => {
  try {
    const totalPackages = db.prepare("SELECT COUNT(*) as count FROM packages").get();
    const totalWaste = db.prepare("SELECT COALESCE(SUM(potential_waste), 0) as total FROM packages").get();
    const priorityCount = db.prepare("SELECT COUNT(*) as count FROM packages WHERE is_priority = 1").get();
    
    res.json({
      success: true,
      data: {
        total_packages: totalPackages.count,
        total_potential_waste: totalWaste.total,
        priority_packages: priorityCount.count
      }
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

const server = app.listen(PORT, () => {
  console.log(`\n🚀 Dashboard backend listening on http://127.0.0.1:${PORT}`);
  console.log(`📁 SQLite database: ${runtimeDbPath}`);
  console.log(`💾 Database size: ${(fs.statSync(runtimeDbPath).size / 1024 / 1024).toFixed(2)} MB`);
  console.log(`\n📊 Available API endpoints:`);
  console.log(`   GET  /api/health                - Health check`);
  console.log(`   GET  /api/stats                 - Basic statistics`);
  console.log(`   GET  /api/bandung/summary       - Bandung Raya summary`);
  console.log(`   GET  /api/bandung/paket         - Filter packages by satker`);
  console.log(`   GET  /api/bandung/satker/codes  - Bandung satker codes`);
  console.log(`   GET  /api/umkm/list             - UMKM list`);
  console.log(`   GET  /api/umkm/match            - Match UMKM with package`);
  console.log(`   GET  /api/umkm/statistik        - UMKM statistics`);
  console.log(``);
});

function shutdown(signal) {
  console.log(`${signal} received, shutting down...`);
  server.close(() => {
    db.close();
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 5000).unref();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));