.mode json
SELECT
  m.name AS mod,
  mf.id AS family,
  s1.id as stat1,
  s1c.name as stat1_category,
  s2.id as stat2,
  s2c.name as stat2_category,
  s3.id as stat3,
  s3c.name as stat3_category,
  s4.id as stat4,
  s4c.name as stat4_category
FROM mods m
JOIN mods_families_junction mfj ON m._language = mfj._language AND m._index = mfj._parent_index
JOIN mod_family mf ON mf._language = 'English' AND mfj.value = mf._index
LEFT JOIN stats s1 ON s1._language = 'English' AND m.stat1 = s1._index
LEFT JOIN passive_skill_stat_categories s1c ON s1c._language = 'English' AND s1.category = s1c._index
LEFT JOIN stats s2 ON s2._language = 'English' AND m.stat2 = s2._index
LEFT JOIN passive_skill_stat_categories s2c ON s2c._language = 'English' AND s2.category = s2c._index
LEFT JOIN stats s3 ON s3._language = 'English' AND m.stat3 = s3._index
LEFT JOIN passive_skill_stat_categories s3c ON s3c._language = 'English' AND s3.category = s3c._index
LEFT JOIN stats s4 ON s4._language = 'English' AND m.stat4 = s4._index
LEFT JOIN passive_skill_stat_categories s4c ON s4c._language = 'English' AND s4.category = s4c._index
WHERE m._language = '$LANG';
