SELECT
  EXTRACT(MONTH FROM data) AS mes,
  ROUND(SUM(CASE WHEN id_loja = 1 THEN subtotalimpressora - valordesconto + valoracrescimo ELSE 0 END), 2) AS "Loja 1",
  ROUND(SUM(CASE WHEN id_loja = 2 THEN subtotalimpressora - valordesconto + valoracrescimo ELSE 0 END), 2) AS "Loja 2",
  ROUND(SUM(CASE WHEN id_loja = 3 THEN subtotalimpressora - valordesconto + valoracrescimo ELSE 0 END), 2) AS "Loja 3"
FROM pdv.venda
WHERE EXTRACT(YEAR FROM data) = 2025
  AND cancelado = false
GROUP BY EXTRACT(MONTH FROM data)
ORDER BY mes;
