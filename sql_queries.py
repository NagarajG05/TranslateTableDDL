HANA_COLUMN_QRY = """
SELECT 
string_agg('"' || column_name ||'"' ,',') as col
FROM "PUBLIC"."VIEW_COLUMNS"
WHERE 
( view_name = :view_name AND view_name NOT LIKE '%hier%' AND view_name NOT LIKE '%dp%')
and schema_name = :schema_name
GROUP BY view_name

UNION

SELECT 
string_agg('"' || column_name ||'"' ,',') as col
FROM "PUBLIC"."TABLE_COLUMNS"
WHERE table_name = :view_name  and schema_name = :schema_name
GROUP BY table_name
"""
