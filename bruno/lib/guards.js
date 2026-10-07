function markerKey(bru) {
  const key = bru.getVar("chaos_marker_key");
  if (typeof key !== "string" || !/^[A-Za-z0-9_.\-]+\/_SUCCESS$/.test(key)) {
    throw new Error("refusing to touch S3: chaos_marker_key is " + JSON.stringify(key) +
      ", not a captured <interval prefix>/_SUCCESS key. Send D1 and D2 first.");
  }
  return key;
}

function markerBaselineGreen(bru) {
  const key = markerKey(bru);
  if (bru.getVar("chaos_marker_baseline_ok") !== true || bru.getVar("chaos_marker_baseline_key") !== key) {
    throw new Error("refusing to delete " + key + ": D5 did not prove the marker check green for this key first");
  }
  return key;
}

function manifestKey(bru) {
  const key = bru.getVar("chaos_manifest_key");
  if (typeof key !== "string" || !/^[A-Za-z0-9_.\-]+\/manifest\.json$/.test(key)) {
    throw new Error("refusing to touch S3: chaos_manifest_key is " + JSON.stringify(key) +
      ", not a captured <interval prefix>/manifest.json key. Send D1, D2 and K1 first.");
  }
  return key;
}

function manifestBaseline(bru) {
  const key = manifestKey(bru);
  const original = bru.getVar("chaos_manifest_original");
  if (bru.getVar("chaos_manifest_baseline_key") !== key || !original || !original.layers ||
      !original.layers.gold || typeof original.layers.gold.row_count !== "number") {
    throw new Error("refusing to overwrite " + key + ": K1 did not capture the manifest stored under this key first");
  }
  return { key: key, original: original };
}

module.exports = { markerKey: markerKey, markerBaselineGreen: markerBaselineGreen, manifestKey: manifestKey, manifestBaseline: manifestBaseline };
