SELECT 
  EXTRACT(MONTH FROM dataemissao) AS mes, --Extrai a data pela data de emissao e cria um grupo
  SUM(valortotal) AS total_mes --soma os valores da coluna valor total
FROM public.notaentrada
WHERE EXTRACT(YEAR FROM dataemissao) = 2025 --define o ano
  AND id_loja = 1
  AND id_tipoentrada !=3
  AND id_fornecedor = 2
GROUP BY EXTRACT(MONTH FROM dataemissao) --agrupa a extração feita 
ORDER BY mes; --ordena por mes 
