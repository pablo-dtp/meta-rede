SELECT
  EXTRACT(MONTH FROM data) AS mes,
  SUM(subtotalimpressora - valordesconto + valoracrescimo) AS totalmes
FROM pdv.venda
WHERE EXTRACT(YEAR FROM data) = 2025
  AND cancelado = false
  AND id_loja = 1
GROUP BY EXTRACT(MONTH FROM data)
ORDER BY mes;
