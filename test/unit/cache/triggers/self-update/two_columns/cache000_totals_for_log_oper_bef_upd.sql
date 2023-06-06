create or replace function cache000_totals_for_log_oper_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if
        new.buy_vat_type is not distinct from old.buy_vat_type
        and
        new.buy_vat_value is not distinct from old.buy_vat_value
        and
        new.sale_vat_type is not distinct from old.sale_vat_type
        and
        new.sale_vat_value is not distinct from old.sale_vat_value
    then
        return new;
    end if;


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

create trigger cache000_totals_for_log_oper_bef_upd
before update of buy_vat_type, buy_vat_value, sale_vat_type, sale_vat_value
on public.log_oper
for each row
execute procedure cache000_totals_for_log_oper_bef_upd();