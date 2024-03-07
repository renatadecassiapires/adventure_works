with
notas_fiscais as (
	select count (distinct id_pedido) as pedidos
	from {{ ref('fct_vendas') }}
	)
select * from notas_fiscais
where pedidos not between 29900 and 31900


