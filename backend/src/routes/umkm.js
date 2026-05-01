const express = require('express');
const fs = require('fs');
const path = require('path');
const router = express.Router();

let umkmData = null;

function loadUmkmData() {
  if (umkmData) return umkmData;
  
  const dataPath = path.join(__dirname, '../../seed/umkm_bandung.json');
  if (fs.existsSync(dataPath)) {
    umkmData = JSON.parse(fs.readFileSync(dataPath, 'utf8'));
    console.log(`📊 Data UMKM dimuat: ${umkmData.metadata?.total_umkm || umkmData.umkm_list?.length || 0} UMKM`);
    return umkmData;
  }
  return { umkm_list: [], metadata: {} };
}

function normalizeText(text) {
  return (text || '').toLowerCase().replace(/[^\w\s]/g, '').replace(/\s+/g, ' ');
}

function matchPaketWithUmkm(paketName, budget) {
  const data = loadUmkmData();
  const umkmList = data.umkm_list || [];
  const text = normalizeText(paketName);
  const budgetNum = parseInt(budget) || 0;
  
  const categoryKeywords = {
    'Elektronik & Komputer': ['komputer', 'laptop', 'printer', 'server', 'pc', 'notebook', 'monitor'],
    'Jasa IT': ['software', 'aplikasi', 'sistem', 'website', 'digital', 'it', 'database'],
    'Alat Kesehatan': ['kesehatan', 'medis', 'obat', 'alkes', 'laboratorium'],
    'Konstruksi & Material': ['bangunan', 'konstruksi', 'material', 'semen', 'besi', 'jalan'],
    'Kendaraan & Transportasi': ['mobil', 'motor', 'kendaraan', 'transport', 'angkutan'],
    'Kuliner & Katering': ['makanan', 'minuman', 'catering', 'katering', 'konsumsi', 'snack'],
    'Percetakan': ['cetak', 'print', 'percetakan', 'brosur', 'spanduk'],
    'Fashion & Aksesoris': ['pakaian', 'seragam', 'batik', 'baju', 'jaket', 'sepatu'],
    'Kerajinan Tangan': ['kerajinan', 'handicraft', 'souvenir', 'craft']
  };
  
  let matchedCategories = [];
  for (const [category, keywords] of Object.entries(categoryKeywords)) {
    for (const keyword of keywords) {
      if (text.includes(keyword)) {
        matchedCategories.push(category);
        break;
      }
    }
  }
  
  matchedCategories = [...new Set(matchedCategories)];
  
  const matchedUmkm = umkmList.filter(umkm => {
    if (matchedCategories.length === 0) return true;
    return matchedCategories.some(cat => umkm.category === cat);
  });
  
  matchedUmkm.sort((a, b) => {
    const diffA = Math.abs((a.capacity || 0) - budgetNum);
    const diffB = Math.abs((b.capacity || 0) - budgetNum);
    return diffA - diffB;
  });
  
  return {
    total_matched: matchedUmkm.length,
    umkm_list: matchedUmkm.slice(0, 10),
    matched_categories: matchedCategories,
    paket_budget: budgetNum,
    paket_name: paketName
  };
}

// ========== API ENDPOINTS ==========

router.get('/list', (req, res) => {
  try {
    const data = loadUmkmData();
    res.json({ success: true, total: data.umkm_list?.length || 0, data: data.umkm_list || [] });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/match', (req, res) => {
  try {
    const { package_name, budget } = req.query;
    if (!package_name) {
      return res.status(400).json({ success: false, error: 'Parameter package_name diperlukan' });
    }
    
    const result = matchPaketWithUmkm(package_name, budget);
    res.json({
      success: true,
      data: result,
      message: result.total_matched > 0 
        ? `Ditemukan ${result.total_matched} UMKM di Bandung yang berpotensi untuk paket ini`
        : 'Belum ada UMKM yang cocok untuk paket ini.'
    });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

router.get('/statistik', (req, res) => {
  try {
    const data = loadUmkmData();
    res.json({ success: true, data: data.statistik || {}, metadata: data.metadata || {} });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

module.exports = router;