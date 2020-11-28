cache totals for log_oper (
    select
        calc_vat( log_oper.buy_vat_type, log_oper.buy_vat_value ) as bay_vat,
        calc_vat( log_oper.sale_vat_type, log_oper.sale_vat_value ) as sale_vat
)