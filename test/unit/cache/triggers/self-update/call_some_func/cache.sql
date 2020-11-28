cache totals for rates (
    select
        rates.quantity * rates.price * calc_vat(
            rates.vat_type,
            rates.vat_value
        ) as total
)