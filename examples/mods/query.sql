.mode json
SELECT
  m.name AS mod,
  mf.id AS family,
  s1.text as stat1,
  s1c.name as stat1_category,
  s2.text as stat2,
  s2c.name as stat2_category,
  s3.text as stat3,
  s3c.name as stat3_category,
  s4.text as stat4,
  s4c.name as stat4_category
FROM mods m
JOIN mods_families_junction mfj ON m._language = mfj._language AND m._index = mfj._parent_index
JOIN mod_family mf ON mfj._language = mf._language AND mfj.value = mf._index
LEFT JOIN stats s1 ON m._language = s1._language AND m.stat1 = s1._index
LEFT JOIN passive_skill_stat_categories s1c ON s1._language = s1c._language AND s1.category = s1c._index
LEFT JOIN stats s2 ON m._language = s2._language AND m.stat2 = s2._index
LEFT JOIN passive_skill_stat_categories s2c ON s2._language = s2c._language AND s2.category = s2c._index
LEFT JOIN stats s3 ON m._language = s3._language AND m.stat3 = s3._index
LEFT JOIN passive_skill_stat_categories s3c ON s3._language = s3c._language AND s3.category = s3c._index
LEFT JOIN stats s4 ON m._language = s4._language AND m.stat4 = s4._index
LEFT JOIN passive_skill_stat_categories s4c ON s4._language = s4c._language AND s4.category = s4c._index
WHERE m._language = '$LANG';
