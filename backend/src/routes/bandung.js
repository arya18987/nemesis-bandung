const express = require('express');
const router = express.Router();

// ========== DATA KODE SATKER BANDUNG RAYA ==========
const BANDUNG_SATKER_CODES = {
  KOTA_BANDUNG: {
    code: "32.73",
    name: "Kota Bandung",
    satkers: [
      "32.73.001", "32.73.002", "32.73.003", "32.73.004", "32.73.005",
      "32.73.006", "32.73.007", "32.73.008", "32.73.009", "32.73.010"
    ]
  },
  KABUPATEN_BANDUNG: {
    code: "32.04",
    name: "Kabupaten Bandung",
    satkers: ["32.04.001", "32.04.002", "32.04.003", "32.04.004"]
  },
  KABUPATEN_BANDUNG_BARAT: {
    code: "32.17",
    name: "Kabupaten Bandung Barat",
    satkers: ["32.17.001", "32.17.002", "32.17.003"]
  }
};

// Fungsi untuk filter paket berdasarkan kode satker
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
  
  if (satkerCode && satkerCode !== 'all') {
    query += ` AND p.satker = ?`;
    params.push(satkerCode);
  }
  
  if (options.bandungRaya === true) {
    query += ` AND (p.satker LIKE '32.73%' OR p.satker LIKE '32.04%' OR p.satker LIKE '32.17%')`;
  }
  
  if (options.wilayah === 'kota_bandung') {
    query += ` AND p.satker LIKE '32.73%'`;
  } else if (options.wilayah === 'kabupaten_bandung') {
    query += ` AND p.satker LIKE '32.04%'`;
  } else if (options.wilayah === 'kabupaten_bandung_barat') {
    query += ` AND p.satker LIKE '32.17%'`;
  }
  
  if (options.severity && ['low', 'med', 'high', 'absurd'].includes(options.severity)) {
    query += ` AND p.severity = ?`;
    params.push(options.severity);
  }
  
  if (options.priorityOnly === true) {
    query += ` AND p.is_priority = 1`;
  }
  
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
    packages: packages
  };
}

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

// ========== API ENDPOINTS ==========

router.get('/summary', (req, res) => {
  try {
    const db = req.app.get('db');
    const summary = getBandungRayaSummary(db);
    res.json({ success: true, data: summary });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/paket', (req, res) => {
  try {
    const db = req.app.get('db');
    const { satker_code, wilayah, severity, priority_only, search, limit, offset } = req.query;
    
    const options = {
      wilayah: wilayah || null,
      severity: severity || null,
      priorityOnly: priority_only === 'true',
      search: search || '',
      limit: parseInt(limit) || 100,
      offset: parseInt(offset) || 0
    };
    
    const result = filterPaketBySatkerCode(db, satker_code || null, options);
    res.json({ success: true, data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/satker/codes', (req, res) => {
  res.json({ success: true, data: BANDUNG_SATKER_CODES });
});

router.get('/kota-bandung', (req, res) => {
  try {
    const db = req.app.get('db');
    const result = filterPaketBySatkerCode(db, null, { wilayah: 'kota_bandung' });
    res.json({ success: true, wilayah: 'Kota Bandung', data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/kab-bandung', (req, res) => {
  try {
    const db = req.app.get('db');
    const result = filterPaketBySatkerCode(db, null, { wilayah: 'kabupaten_bandung' });
    res.json({ success: true, wilayah: 'Kabupaten Bandung', data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/kab-bandung-barat', (req, res) => {
  try {
    const db = req.app.get('db');
    const result = filterPaketBySatkerCode(db, null, { wilayah: 'kabupaten_bandung_barat' });
    res.json({ success: true, wilayah: 'Kabupaten Bandung Barat', data: result });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;