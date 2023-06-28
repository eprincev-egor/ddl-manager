create or replace function cache000_totals_for_rates_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if
        new.price is not distinct from old.price
        and
        new.quantity is not distinct from old.quantity
        and
        new.vat_type is not distinct from old.vat_type
        and
        new.vat_value is not distinct from old.vat_value
    then
        return new;
    end if;


    new.total = new.quantity * new.price *     calc_vat(
        new.vat_type,
        new.vat_value
        );

    return new;
end
$body$
language plpgsql;

create trigger cache000_totals_for_rates_bef_upd
before update of price, quantity, vat_type, vat_value
on public.rates
for each row
execute procedure cache000_totals_for_rates_bef_upd();