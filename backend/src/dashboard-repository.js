const { DEFAULT_REGION_PAGE_SIZE, MAX_REGION_PAGE_SIZE } = require("./config");

const LEGEND_COLORS = ["#7b86a3", "#b5a882", "#d4a999", "#8b7332", "#a83c2e"];
const VALID_OWNER_TYPES = ["kabkota", "provinsi", "central", "other"];
const VALID_SEVERITIES = ["low", "med", "high", "absurd"];
const OWNER_METRIC_DEFINITIONS = [
  {
    key: "central",
    countField: "central_packages",
    priorityField: "central_priority_packages",
    wasteField: "central_potential_waste",
    budgetField: "central_budget",
  },
  {
    key: "provinsi",
    countField: "provincial_packages",
    priorityField: "provincial_priority_packages",
    wasteField: "provincial_potential_waste",
    budgetField: "provincial_budget",
  },
  {
    key: "kabkota",
    countField: "local_packages",
    priorityField: "local_priority_packages",
    wasteField: "local_potential_waste",
    budgetField: "local_budget",
  },
  {
    key: "other",
    countField: "other_packages",
    priorityField: "other_priority_packages",
    wasteField: "other_potential_waste",
    budgetField: "other_budget",
  },
];

// ========== FITUR TAMBAHAN: FILTER KODE SATKER BANDUNG RAYA ==========

// Kode Satker untuk Wilayah Bandung Raya
const BANDUNG_SATKER_CODES = {
  KOTA_BANDUNG: {
    code: "32.73",
    name: "Kota Bandung",
    satkers: [
      "32.73.001", // Dinas Pendidikan
      "32.73.002", // Dinas Pekerjaan Umum
      "32.73.003", // Dinas Kesehatan
      "32.73.004", // Dinas Perhubungan
      "32.73.005", // Dinas Sosial
      "32.73.006", // Dinas Pemuda dan Olahraga
      "32.73.007", // Dinas Lingkungan Hidup
      "32.73.008", // Dinas Kependudukan dan Catatan Sipil
      "32.73.009", // Dinas Perumahan dan Kawasan Permukiman
      "32.73.010", // Dinas Komunikasi dan Informatika
    ]
  },
  KABUPATEN_BANDUNG: {
    code: "32.04",
    name: "Kabupaten Bandung",
    satkers: [
      "32.04.001", // Dinas Pendidikan
      "32.04.002", // Dinas Pekerjaan Umum
      "32.04.003", // Dinas Kesehatan
      "32.04.004", // Dinas Pertanian
    ]
  },
  KABUPATEN_BANDUNG_BARAT: {
    code: "32.17",
    name: "Kabupaten Bandung Barat",
    satkers: [
      "32.17.001", // Dinas Pendidikan
      "32.17.002", // Dinas Pekerjaan Umum
      "32.17.003", // Dinas Kesehatan
    ]
  }
};

// Filter paket berdasarkan Kode Satker (untuk Bandung Raya)
function filterPaketBySatkerCode(db, satkerCode, options = {}) {
  let query = `
    SELECT 
      p.id,
      p.package_name,
      p.owner_name,
      p.owner_type,
      p.satker,
      p.budget,
      p.potential_waste,
      p.severity,
      p.reason,
      p.is_priority,
      p.risk_score,
      p.location_raw
    FROM packages p
    WHERE 1=1
  `;
  
  const params = [];
  
  // Filter berdasarkan kode satker spesifik
  if (satkerCode && satkerCode !== 'all') {
    query += ` AND p.satker = ?`;
    params.push(satkerCode);
  }
  
  // Filter berdasarkan wilayah Bandung Raya (prefix kode)
  if (options.bandungRaya === true) {
    query += ` AND (p.satker LIKE '32.73%' OR p.satker LIKE '32.04%' OR p.satker LIKE '32.17%')`;
  }
  
  // Filter berdasarkan wilayah spesifik
  if (options.wilayah === 'kota_bandung') {
    query += ` AND p.satker LIKE '32.73%'`;
  } else if (options.wilayah === 'kabupaten_bandung') {
    query += ` AND p.satker LIKE '32.04%'`;
  } else if (options.wilayah === 'kabupaten_bandung_barat') {
    query += ` AND p.satker LIKE '32.17%'`;
  }
  
  // Filter berdasarkan tingkat keparahan
  if (options.severity && VALID_SEVERITIES.includes(options.severity)) {
    query += ` AND p.severity = ?`;
    params.push(options.severity);
  }
  
  // Filter prioritas saja
  if (options.priorityOnly === true) {
    query += ` AND p.is_priority = 1`;
  }
  
  // Filter berdasarkan pencarian teks
  if (options.search) {
    query += ` AND (p.package_name LIKE ? OR p.owner_name LIKE ?)`;
    const searchPattern = `%${options.search}%`;
    params.push(searchPattern, searchPattern);
  }
  
  query += ` ORDER BY p.potential_waste DESC, p.budget DESC LIMIT ? OFFSET ?`;
  
  const limit = options.limit || 100;
  const offset = options.offset || 0;
  params.push(limit, offset);
  
  const packages = db.prepare(query).all(...params);
  
  // Hitung agregat
  const stats = db.prepare(`
    SELECT 
      COUNT(*) as total_packages,
      COALESCE(SUM(p.budget), 0) as total_budget,
      COALESCE(SUM(p.potential_waste), 0) as total_potential_waste,
      SUM(CASE WHEN p.severity = 'high' THEN 1 ELSE 0 END) as high_severity_count,
      SUM(CASE WHEN p.severity = 'absurd' THEN 1 ELSE 0 END) as absurd_severity_count,
      SUM(CASE WHEN p.severity = 'med' THEN 1 ELSE 0 END) as med_severity_count,
      SUM(CASE WHEN p.is_priority = 1 THEN 1 ELSE 0 END) as priority_count
    FROM packages p
    WHERE ${query.split('WHERE')[1].split('ORDER BY')[0]}
  `).all(...params.slice(0, -2))[0];
  
  return {
    total_packages: stats?.total_packages || 0,
    total_budget: stats?.total_budget || 0,
    total_potential_waste: stats?.total_potential_waste || 0,
    severity_counts: {
      med: stats?.med_severity_count || 0,
      high: stats?.high_severity_count || 0,
      absurd: stats?.absurd_severity_count || 0,
    },
    priority_count: stats?.priority_count || 0,
    packages: packages,
    wilayah_info: options.wilayah ? BANDUNG_SATKER_CODES[options.wilayah.toUpperCase()] : null
  };
}

// Daftar semua Satker yang tersedia di database
function getAllSatkerList(db) {
  const satkers = db.prepare(`
    SELECT DISTINCT 
      satker, 
      owner_name,
      COUNT(*) as package_count,
      COALESCE(SUM(budget), 0) as total_budget
    FROM packages 
    WHERE satker IS NOT NULL AND satker != ''
    GROUP BY satker
    ORDER BY total_budget DESC
  `).all();
  
  return satkers;
}

// Ringkasan untuk Bandung Raya (dashboard khusus)
function getBandungRayaSummary(db) {
  const wilayahList = ['kota_bandung', 'kabupaten_bandung', 'kabupaten_bandung_barat'];
  const result = {};
  
  for (const wilayah of wilayahList) {
    const data = filterPaketBySatkerCode(db, null, { wilayah: wilayah });
    result[wilayah] = {
      name: BANDUNG_SATKER_CODES[wilayah.toUpperCase()]?.name || wilayah,
      total_packages: data.total_packages,
      total_budget: data.total_budget,
      total_potential_waste: data.total_potential_waste,
      severity_counts: data.severity_counts,
      priority_count: data.priority_count
    };
  }
  
  // Total Bandung Raya
  const allData = filterPaketBySatkerCode(db, null, { bandungRaya: true });
  result.total_bandung_raya = {
    name: "Bandung Raya",
    total_packages: allData.total_packages,
    total_budget: allData.total_budget,
    total_potential_waste: allData.total_potential_waste,
    severity_counts: allData.severity_counts,
    priority_count: allData.priority_count
  };
  
  return result;
}

// ========== KODE ASLI (lanjutan) ==========

function getJsonAsset(db, key, fallback) {
  const row = db.prepare("SELECT json FROM assets WHERE key = ?").get(key);
  return row ? JSON.parse(row.json) : fallback;
}

function clampInteger(value, defaultValue, minimum, maximum) {
  const parsed = Number.parseInt(value, 10);

  if (!Number.isFinite(parsed)) {
    return defaultValue;
  }

  return Math.min(Math.max(parsed, minimum), maximum);
}

function parseBooleanQuery(value) {
  if (value === undefined || value === null || value === "") {
    return false;
  }

  if (typeof value === "boolean") {
    return value;
  }

  const normalized = String(value).trim().toLowerCase();
  return ["1", "true", "yes", "ya"].includes(normalized);
}

function escapeLikePattern(value) {
  return String(value).replace(/[\\%_]/g, (match) => `\\${match}`);
}

function dominantOwnerType(row) {
  const counts = [
    { key: "central", value: row.central_packages || 0 },
    { key: "provinsi", value: row.provincial_packages || 0 },
    { key: "kabkota", value: row.local_packages || 0 },
    { key: "other", value: row.other_packages || 0 },
  ].sort((left, right) => right.value - left.value);

  return counts[0].value > 0 ? counts[0].key : null;
}

function buildOwnerMetrics(row) {
  return OWNER_METRIC_DEFINITIONS.reduce((metrics, definition) => {
    metrics[definition.key] = {
      totalPackages: row[definition.countField] || 0,
      totalPriorityPackages: row[definition.priorityField] || 0,
      totalPotentialWaste: row[definition.wasteField] || 0,
      totalBudget: row[definition.budgetField] || 0,
    };

    return metrics;
  }, {});
}

function buildProvinceOwnerMetrics(row) {
  return {
    central: {
      totalPackages: 0,
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
    },
    provinsi: {
      totalPackages: row.total_packages || 0,
      totalPriorityPackages: row.total_priority_packages || 0,
      totalPotentialWaste: row.total_potential_waste || 0,
      totalBudget: row.total_budget || 0,
    },
    kabkota: {
      totalPackages: 0,
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
    },
    other: {
      totalPackages: 0,
      totalPriorityPackages: 0,
      totalPotentialWaste: 0,
      totalBudget: 0,
    },
  };
}

function mapOwnerRow(row) {
  return {
    ownerType: row.owner_type,
    ownerName: row.owner_name,
    totalPackages: row.total_packages,
    totalPriorityPackages: row.total_priority_packages,
    totalFlaggedPackages: row.total_flagged_packages,
    totalPotentialWaste: row.total_potential_waste,
    totalBudget: row.total_budget,
    severityCounts: {
      med: row.med_severity_packages,
      high: row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
  };
}

function mapRegionRow(row) {
  return {
    regionKey: row.region_key,
    code: row.code,
    provinceName: row.province_name,
    regionName: row.region_name,
    regionType: row.region_type,
    displayName: row.display_name,
    totalPackages: row.total_packages,
    totalPriorityPackages: row.total_priority_packages,
    totalFlaggedPackages: row.total_flagged_packages,
    totalPotentialWaste: row.total_potential_waste,
    totalBudget: row.total_budget,
    avgRiskScore: Number((row.avg_risk_score || 0).toFixed(2)),
    maxRiskScore: row.max_risk_score,
    ownerMix: {
      central: row.central_packages,
      provinsi: row.provincial_packages,
      kabkota: row.local_packages,
      other: row.other_packages,
    },
    ownerMetrics: buildOwnerMetrics(row),
    severityCounts: {
      med: row.med_severity_packages,
      high: row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
    dominantOwnerType: dominantOwnerType(row),
  };
}

function mapProvinceRow(row) {
  return {
    provinceKey: row.province_key,
    code: row.code,
    provinceName: row.province_name,
    regionName: row.province_name,
    regionType: "Provinsi",
    displayName: row.display_name,
    totalPackages: row.total_packages,
    totalPriorityPackages: row.total_priority_packages,
    totalFlaggedPackages: row.total_flagged_packages,
    totalPotentialWaste: row.total_potential_waste,
    totalBudget: row.total_budget,
    avgRiskScore: Number((row.avg_risk_score || 0).toFixed(2)),
    maxRiskScore: row.max_risk_score,
    ownerMix: {
      central: 0,
      provinsi: row.total_packages,
      kabkota: 0,
      other: 0,
    },
    ownerMetrics: buildProvinceOwnerMetrics(row),
    severityCounts: {
      med: row.med_severity_packages,
      high: row.high_severity_packages,
      absurd: row.absurd_severity_packages,
    },
    dominantOwnerType: row.total_packages > 0 ? "provinsi" : null,
  };
}

function buildLegend(values) {
  const positiveValues = values.filter((value) => value > 0).sort((left, right) => left - right);
  const ranges = [];

  if (!positiveValues.length) {
    return {
      zeroColor: "#243155",
      ranges,
    };
  }

  const quantiles = [0.2, 0.4, 0.6, 0.8, 1].map((ratio) => {
    const index = Math.min(positiveValues.length - 1, Math.floor((positiveValues.length - 1) * ratio));
    return positiveValues[index];
  });

  let minimum = positiveValues[0];

  for (let index = 0; index < quantiles.length; index += 1) {
    const maximum = quantiles[index];

    if (maximum < minimum) {
      continue;
    }

    if (ranges.length && maximum === ranges[ranges.length - 1].max) {
      continue;
    }

    ranges.push({
      key: `band-${index + 1}`,
      color: LEGEND_COLORS[Math.min(index, LEGEND_COLORS.length - 1)],
      min: minimum,
      max: maximum,
    });

    minimum = maximum + 0.01;
  }

  return {
    zeroColor: "#243155",
    ranges,
  };
}

function getNationalSummary(db) {
  return db
    .prepare(`
      SELECT
        COUNT(*) AS total_packages,
        COALESCE(SUM(is_priority), 0) AS total_priority_packages,
        COALESCE(ROUND(SUM(potential_waste), 2), 0) AS total_potential_waste,
        COALESCE(SUM(COALESCE(budget, 0)), 0) AS total_budget,
        COALESCE(SUM(CASE WHEN mapped_region_count = 0 THEN 1 ELSE 0 END), 0) AS unmapped_packages,
        COALESCE(SUM(CASE WHEN mapped_region_count > 1 THEN 1 ELSE 0 END), 0) AS multi_location_packages
      FROM packages
    `)
    .get();
}

function getRegionRows(db) {
  return db
    .prepare(`
      SELECT
        regions.region_key,
        regions.code,
        regions.province_name,
        regions.region_name,
        regions.region_type,
        regions.display_name,
        region_metrics.total_packages,
        region_metrics.total_priority_packages,
        region_metrics.total_flagged_packages,
        region_metrics.total_potential_waste,
        region_metrics.total_budget,
        region_metrics.avg_risk_score,
        region_metrics.max_risk_score,
        region_metrics.central_packages,
        region_metrics.provincial_packages,
        region_metrics.local_packages,
        region_metrics.other_packages,
        region_metrics.central_priority_packages,
        region_metrics.provincial_priority_packages,
        region_metrics.local_priority_packages,
        region_metrics.other_priority_packages,
        region_metrics.central_potential_waste,
        region_metrics.provincial_potential_waste,
        region_metrics.local_potential_waste,
        region_metrics.other_potential_waste,
        region_metrics.central_budget,
        region_metrics.provincial_budget,
        region_metrics.local_budget,
        region_metrics.other_budget,
        region_metrics.med_severity_packages,
        region_metrics.high_severity_packages,
        region_metrics.absurd_severity_packages
      FROM regions
      INNER JOIN region_metrics ON region_metrics.region_key = regions.region_key
      ORDER BY
        region_metrics.total_potential_waste DESC,
        region_metrics.total_priority_packages DESC,
        region_metrics.total_packages DESC,
        regions.display_name ASC
    `)
    .all();
}

function getProvinceRows(db) {
  return db
    .prepare(`
      SELECT
        provinces.province_key,
        provinces.code,
        provinces.province_name,
        provinces.display_name,
        province_metrics.total_packages,
        province_metrics.total_priority_packages,
        province_metrics.total_flagged_packages,
        province_metrics.total_potential_waste,
        province_metrics.total_budget,
        province_metrics.avg_risk_score,
        province_metrics.max_risk_score,
        province_metrics.med_severity_packages,
        province_metrics.high_severity_packages,
        province_metrics.absurd_severity_packages
      FROM provinces
      INNER JOIN province_metrics ON province_metrics.province_key = provinces.province_key
      ORDER BY
        province_metrics.total_potential_waste DESC,
        province_metrics.total_priority_packages DESC,
        province_metrics.total_packages DESC,
        provinces.display_name ASC
    `)
    .all();
}

function getOwnerRows(db, ownerType) {
  return db
    .prepare(`
      SELECT
        owner_metrics.owner_type,
        owner_metrics.owner_name,
        owner_metrics.total_packages,
        owner_metrics.total_priority_packages,
        owner_metrics.total_flagged_packages,
        owner_metrics.total_potential_waste,
        owner_metrics.total_budget,
        owner_metrics.med_severity_packages,
        owner_metrics.high_severity_packages,
        owner_metrics.absurd_severity_packages
      FROM owner_metrics
      WHERE owner_metrics.owner_type = ?
      ORDER BY
        owner_metrics.total_potential_waste DESC,
        owner_metrics.total_priority_packages DESC,
        owner_metrics.total_packages DESC,
        owner_metrics.owner_name ASC
    `)
    .all(ownerType);
}

function normalizeScopedPackageQuery(requestQuery, options = {}) {
  return {
    page: clampInteger(requestQuery.page, 1, 1, Number.MAX_SAFE_INTEGER),
    pageSize: clampInteger(requestQuery.pageSize, DEFAULT_REGION_PAGE_SIZE, 1, MAX_REGION_PAGE_SIZE),
    search: (requestQuery.search || "").trim(),
    ownerType: options.allowOwnerType === false ? "" : (requestQuery.ownerType || "").trim(),
    severity: options.allowSeverity === false ? "" : (requestQuery.severity || "").trim(),
    priorityOnly: parseBooleanQuery(requestQuery.priorityOnly),
  };
}

function buildPackagesWhereClause(scopeColumn, scopeKey, query, options = {}) {
  const clauses = [`${scopeColumn} = ?`];
  const params = [scopeKey];

  if (query.search) {
    const searchValue = `%${escapeLikePattern(query.search)}%`;
    clauses.push(
      "(packages.package_name LIKE ? ESCAPE '\\' OR packages.owner_name LIKE ? ESCAPE '\\' OR COALESCE(packages.satker, '') LIKE ? ESCAPE '\\')"
    );
    params.push(searchValue, searchValue, searchValue);
  }

  if (options.forcedOwnerType) {
    clauses.push("packages.owner_type = ?");
    params.push(options.forcedOwnerType);
  } else if (VALID_OWNER_TYPES.includes(query.ownerType)) {
    clauses.push("packages.owner_type = ?");
    params.push(query.ownerType);
  }

  if (options.allowSeverity !== false && VALID_SEVERITIES.includes(query.severity)) {
    clauses.push("packages.severity = ?");
    params.push(query.severity);
  }

  if (query.priorityOnly) {
    clauses.push("packages.is_priority = 1");
  }

  return {
    sql: clauses.join(" AND "),
    params,
  };
}

function buildOwnerPackagesWhereClause(ownerType, ownerName, query) {
  const clauses = ["packages.owner_type = ?", "packages.owner_name = ?"];
  const params = [ownerType, ownerName];

  if (query.search) {
    const searchValue = `%${escapeLikePattern(query.search)}%`;
    clauses.push(
      "(packages.package_name LIKE ? ESCAPE '\\' OR packages.owner_name LIKE ? ESCAPE '\\' OR COALESCE(packages.satker, '') LIKE ? ESCAPE '\\')"
    );
    params.push(searchValue, searchValue, searchValue);
  }

  if (VALID_SEVERITIES.includes(query.severity)) {
    clauses.push("packages.severity = ?");
    params.push(query.severity);
  }

  if (query.priorityOnly) {
    clauses.push("packages.is_priority = 1");
  }

  return {
    sql: clauses.join(" AND "),
    params,
  };
}

function mapPackageRow(row) {
  return {
    id: row.id,
    sourceId: row.source_id,
    packageName: row.package_name,
    ownerName: row.owner_name,
    ownerType: row.owner_type,
    satker: row.satker,
    locationRaw: row.location_raw,
    budget: row.budget,
    fundingSource: row.funding_source,
    procurementType: row.procurement_type,
    procurementMethod: row.procurement_method,
    selectionDate: row.selection_date,
    audit: {
      schemaVersion: row.schema_version,
      severity: row.severity,
      potensiPemborosan: row.potential_waste,
      reason: row.reason,
      flags: {
        isMencurigakan: row.is_mencurigakan === null ? null : Boolean(row.is_mencurigakan),
        isPemborosan: row.is_pemborosan === null ? null : Boolean(row.is_pemborosan),
      },
    },
    meta: {
      isPriority: Boolean(row.is_priority),
      isFlagged: Boolean(row.is_flagged),
      riskScore: row.risk_score,
      activeTagCount: row.active_tag_count,
      mappedRegionCount: row.mapped_region_count,
    },
  };
}

function queryPackagesPage(db, scopeTable, scopeColumn, scopeKey, normalizedQuery, options = {}) {
  const whereClause = buildPackagesWhereClause(scopeColumn, scopeKey, normalizedQuery, options);
  const countRow = db
    .prepare(`
      SELECT COUNT(*) AS total
      FROM ${scopeTable}
      INNER JOIN packages ON packages.id = ${scopeTable}.package_id
      WHERE ${whereClause.sql}
    `)
    .get(...whereClause.params);
  const totalItems = countRow.total || 0;
  const totalPages = totalItems ? Math.ceil(totalItems / normalizedQuery.pageSize) : 1;
  const page = Math.min(normalizedQuery.page, totalPages);
  const offset = (page - 1) * normalizedQuery.pageSize;
  const rows = db
    .prepare(`
      SELECT
        packages.id,
        packages.source_id,
        packages.schema_version,
        packages.owner_name,
        packages.owner_type,
        packages.satker,
        packages.package_name,
        packages.location_raw,
        packages.budget,
        packages.funding_source,
        packages.procurement_type,
        packages.procurement_method,
        packages.selection_date,
        packages.potential_waste,
        packages.severity,
        packages.reason,
        packages.is_mencurigakan,
        packages.is_pemborosan,
        packages.risk_score,
        packages.active_tag_count,
        packages.is_priority,
        packages.is_flagged,
        packages.mapped_region_count
      FROM ${scopeTable}
      INNER JOIN packages ON packages.id = ${scopeTable}.package_id
      WHERE ${whereClause.sql}
      ORDER BY
        packages.is_priority DESC,
        packages.potential_waste DESC,
        packages.risk_score DESC,
        COALESCE(packages.budget, 0) DESC,
        packages.inserted_order ASC
      LIMIT ? OFFSET ?
    `)
    .all(...whereClause.params, normalizedQuery.pageSize, offset)
    .map(mapPackageRow);

  return {
    totalItems,
    page,
    pageSize: normalizedQuery.pageSize,
    totalPages,
    rows,
  };
}

function queryOwnerPackagesPage(db, ownerType, ownerName, normalizedQuery) {
  const whereClause = buildOwnerPackagesWhereClause(ownerType, ownerName, normalizedQuery);
  const countRow = db
    .prepare(`
      SELECT COUNT(*) AS total
      FROM packages
      WHERE ${whereClause.sql}
    `)
    .get(...whereClause.params);
  const totalItems = countRow.total || 0;
  const totalPages = totalItems ? Math.ceil(totalItems / normalizedQuery.pageSize) : 1;
  const page = Math.min(normalizedQuery.page, totalPages);
  const offset = (page - 1) * normalizedQuery.pageSize;
  const rows = db
    .prepare(`
      SELECT
        packages.id,
        packages.source_id,
        packages.schema_version,
        packages.owner_name,
        packages.owner_type,
        packages.satker,
        packages.package_name,
        packages.location_raw,
        packages.budget,
        packages.funding_source,
        packages.procurement_type,
        packages.procurement_method,
        packages.selection_date,
        packages.potential_waste,
        packages.severity,
        packages.reason,
        packages.is_mencurigakan,
        packages.is_pemborosan,
        packages.risk_score,
        packages.active_tag_count,
        packages.is_priority,
        packages.is_flagged,
        packages.mapped_region_count
      FROM packages
      WHERE ${whereClause.sql}
      ORDER BY
        packages.is_priority DESC,
        packages.potential_waste DESC,
        packages.risk_score DESC,
        COALESCE(packages.budget, 0) DESC,
        packages.inserted_order ASC
      LIMIT ? OFFSET ?
    `)
    .all(...whereClause.params, normalizedQuery.pageSize, offset)
    .map(mapPackageRow);

  return {
    totalItems,
    page,
    pageSize: normalizedQuery.pageSize,
    totalPages,
    rows,
  };
}

function getBootstrapPayload(db) {
  const summaryRow = getNationalSummary(db);
  const regions = getRegionRows(db).map(mapRegionRow);
  const provinces = getProvinceRows(db).map(mapProvinceRow);
  const centralOwners = getOwnerRows(db, "central").map(mapOwnerRow);

  return {
    summary: {
      totalPackages: summaryRow.total_packages || 0,
      totalPriorityPackages: summaryRow.total_priority_packages || 0,
      totalPotentialWaste: summaryRow.total_potential_waste || 0,
      totalBudget: summaryRow.total_budget || 0,
      unmappedPackages: summaryRow.unmapped_packages || 0,
      multiLocationPackages: summaryRow.multi_location_packages || 0,
    },
    legend: buildLegend(regions.map((region) => region.totalPotentialWaste)),
    geo: getJsonAsset(db, "audit_geojson", { type: "FeatureCollection", features: [] }),
    regions,
    provinceView: {
      legend: buildLegend(provinces.map((province) => province.totalPotentialWaste)),
      geo: getJsonAsset(db, "audit_province_geojson", { type: "FeatureCollection", features: [] }),
      provinces,
    },
    ownerLists: {
      central: centralOwners,
    },
  };
}

function getRegionPackages(db, regionKey, requestQuery) {
  const regionRow = db
    .prepare(`
      SELECT
        regions.region_key,
        regions.code,
        regions.province_name,
        regions.region_name,
        regions.region_type,
        regions.display_name,
        region_metrics.total_packages,
        region_metrics.total_priority_packages,
        region_metrics.total_flagged_packages,
        region_metrics.total_potential_waste,
        region_metrics.total_budget,
        region_metrics.avg_risk_score,
        region_metrics.max_risk_score,
        region_metrics.central_packages,
        region_metrics.provincial_packages,
        region_metrics.local_packages,
        region_metrics.other_packages,
        region_metrics.central_priority_packages,
        region_metrics.provincial_priority_packages,
        region_metrics.local_priority_packages,
        region_metrics.other_priority_packages,
        region_metrics.central_potential_waste,
        region_metrics.provincial_potential_waste,
        region_metrics.local_potential_waste,
        region_metrics.other_potential_waste,
        region_metrics.central_budget,
        region_metrics.provincial_budget,
        region_metrics.local_budget,
        region_metrics.other_budget,
        region_metrics.med_severity_packages,
        region_metrics.high_severity_packages,
        region_metrics.absurd_severity_packages
      FROM regions
      INNER JOIN region_metrics ON region_metrics.region_key = regions.region_key
      WHERE regions.region_key = ?
    `)
    .get(regionKey);

  if (!regionRow) {
    return null;
  }

  const normalizedQuery = normalizeScopedPackageQuery(requestQuery);
  const pageResult = queryPackagesPage(db, "package_regions", "package_regions.region_key", regionKey, normalizedQuery);

  return {
    region: mapRegionRow(regionRow),
    summary: {
      totalItems: pageResult.totalItems,
      filteredItems: pageResult.totalItems,
    },
    pagination: {
      page: pageResult.page,
      pageSize: pageResult.pageSize,
      totalItems: pageResult.totalItems,
      totalPages: pageResult.totalPages,
    },
    filters: {
      search: normalizedQuery.search,
      ownerType: normalizedQuery.ownerType,
      severity: normalizedQuery.severity,
      priorityOnly: normalizedQuery.priorityOnly,
    },
    items: pageResult.rows,
  };
}

function getProvincePackages(db, provinceKey, requestQuery) {
  const provinceRow = db
    .prepare(`
      SELECT
        provinces.province_key,
        provinces.code,
        provinces.province_name,
        provinces.display_name,
        province_metrics.total_packages,
        province_metrics.total_priority_packages,
        province_metrics.total_flagged_packages,
        province_metrics.total_potential_waste,
        province_metrics.total_budget,
        province_metrics.avg_risk_score,
        province_metrics.max_risk_score,
        province_metrics.med_severity_packages,
        province_metrics.high_severity_packages,
        province_metrics.absurd_severity_packages
      FROM provinces
      INNER JOIN province_metrics ON province_metrics.province_key = provinces.province_key
      WHERE provinces.province_key = ?
    `)
    .get(provinceKey);

  if (!provinceRow) {
    return null;
  }

  const normalizedQuery = normalizeScopedPackageQuery(requestQuery, {
    allowOwnerType: false,
  });
  const pageResult = queryPackagesPage(
    db,
    "package_provinces",
    "package_provinces.province_key",
    provinceKey,
    normalizedQuery,
    {
      forcedOwnerType: "provinsi",
    }
  );

  return {
    province: mapProvinceRow(provinceRow),
    summary: {
      totalItems: pageResult.totalItems,
      filteredItems: pageResult.totalItems,
    },
    pagination: {
      page: pageResult.page,
      pageSize: pageResult.pageSize,
      totalItems: pageResult.totalItems,
      totalPages: pageResult.totalPages,
    },
    filters: {
      search: normalizedQuery.search,
      severity: normalizedQuery.severity,
      priorityOnly: normalizedQuery.priorityOnly,
    },
    items: pageResult.rows,
  };
}

function getOwnerPackages(db, requestQuery) {
  const ownerType = (requestQuery.ownerType || "").trim();
  const ownerName = (requestQuery.ownerName || "").trim();

  if (!VALID_OWNER_TYPES.includes(ownerType) || !ownerName) {
    return null;
  }

  const ownerRow = db
    .prepare(`
      SELECT
        owner_metrics.owner_type,
        owner_metrics.owner_name,
        owner_metrics.total_packages,
        owner_metrics.total_priority_packages,
        owner_metrics.total_flagged_packages,
        owner_metrics.total_potential_waste,
        owner_metrics.total_budget,
        owner_metrics.med_severity_packages,
        owner_metrics.high_severity_packages,
        owner_metrics.absurd_severity_packages
      FROM owner_metrics
      WHERE owner_metrics.owner_type = ?
        AND owner_metrics.owner_name = ?
    `)
    .get(ownerType, ownerName);

  if (!ownerRow) {
    return null;
  }

  const normalizedQuery = normalizeScopedPackageQuery(requestQuery, {
    allowOwnerType: false,
  });
  const pageResult = queryOwnerPackagesPage(db, ownerType, ownerName, normalizedQuery);

  return {
    owner: mapOwnerRow(ownerRow),
    summary: {
      totalItems: pageResult.totalItems,
      filteredItems: pageResult.totalItems,
    },
    pagination: {
      page: pageResult.page,
      pageSize: pageResult.pageSize,
      totalItems: pageResult.totalItems,
      totalPages: pageResult.totalPages,
    },
    filters: {
      search: normalizedQuery.search,
      severity: normalizedQuery.severity,
      priorityOnly: normalizedQuery.priorityOnly,
    },
    items: pageResult.rows,
  };
}

module.exports = {
  getBootstrapPayload,
  getOwnerPackages,
  getRegionPackages,
  getProvincePackages,
  // EKSPOR FUNGSI BARU UNTUK FILTER SATKER BANDUNG
  filterPaketBySatkerCode,
  getAllSatkerList,
  getBandungRayaSummary,
  BANDUNG_SATKER_CODES
};