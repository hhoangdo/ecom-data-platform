SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SELECT catalog_name, schema_name
FROM iceberg.information_schema.schemata
ORDER BY schema_name;
