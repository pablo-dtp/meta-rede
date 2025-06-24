SELECT DISTINCT ni.id_produto,  pf.codigoexterno
FROM public.notaentrada ne
JOIN public.notaentradaitem ni ON ne.id = ni.id_notaentrada
JOIN public.produtofornecedor pf ON ni.id_produto = pf.id_produto
WHERE pf.id_fornecedor = 2 
AND ne.id_fornecedor = 2
AND ne.id_tipoentrada != 3
AND ne.dataemissao BETWEEN '2025-05-01' AND '2025-05-31'
AND ne.id_loja = 1
ORDER BY ni.id_produto ASC;