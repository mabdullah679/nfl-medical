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

module.exports = { markerKey: markerKey, markerBaselineGreen: markerBaselineGreen };
