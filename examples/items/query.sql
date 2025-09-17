.mode json
SELECT
  bit.name AS item_name,
  ic.name AS class_name,
  cc.text as class_category,
  cec.name AS exchange_category,
  cecs.name AS exchange_sub_category,
  ce.gold_purchase_fee as price,
  CASE
    WHEN COUNT(DISTINCT t.id) > 0
    THEN json_group_array(DISTINCT t.id)
    ELSE json_array()
  END AS tags
FROM base_item_types bit
LEFT JOIN item_classes ic ON bit._language = ic._language AND bit.item_class = ic._index
LEFT JOIN currency_exchange ce ON bit._language = ce._language AND bit._index = ce.item
LEFT JOIN currency_exchange_categories cec ON ce.category = cec._index AND ce._language = cec._language
LEFT JOIN currency_exchange_categories cecs ON ce.sub_category = cecs._index AND ce._language = cecs._language
LEFT JOIN item_class_categories cc ON ic._language = cc._language AND ic.item_class_category = cc._index
LEFT JOIN base_item_types_tags_junction btj ON bit._language = btj._language AND bit._index = btj._parent_index
LEFT JOIN tags t ON btj._language = t._language AND btj.value = t._index
WHERE bit._language = '$LANG'
GROUP BY bit._language, bit._index
ORDER BY ic.name, bit.name;
