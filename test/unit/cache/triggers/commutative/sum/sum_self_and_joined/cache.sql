cache totals for companies (
    select
        sum( orders.profit * vats.vat_value ) as orders_total
    from orders

    left join vats on
        vats.id = orders.id_vat
    
    where
        orders.id_client = companies.id
)