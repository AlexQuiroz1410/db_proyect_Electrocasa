SELECT
    'gld_ventas_sucursal_mes' AS tabla,
    COUNT(*) AS registros
FROM dbelectrocasa.gold.gld_ventas_sucursal_mes

UNION ALL

SELECT
    'gld_ranking_productos',
    COUNT(*)
FROM dbelectrocasa.gold.gld_ranking_productos

UNION ALL

SELECT
    'gld_dotacion_sucursal',
    COUNT(*)
FROM dbelectrocasa.gold.gld_dotacion_sucursal

UNION ALL

SELECT
    'gld_resenas_categoria',
    COUNT(*)
FROM dbelectrocasa.gold.gld_resenas_categoria;