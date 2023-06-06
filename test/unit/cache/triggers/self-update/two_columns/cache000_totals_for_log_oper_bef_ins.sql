create or replace function cache000_totals_for_log_oper_bef_ins()
returns trigger as $body$
begin
    new.buy_vat = calc_vat(
        new.buy_vat_type,
        new.buy_vat_value
    );
    new.sale_vat = calc_vat(
        new.sale_vat_type,
        new.sale_vat_value
    );

    return new;
end
$body$
language plpgsql;

create trigger cache000_totals_for_log_oper_bef_ins
before insert
on public.log_oper
for each row
execute procedure cache000_totals_for_log_oper_bef_ins();