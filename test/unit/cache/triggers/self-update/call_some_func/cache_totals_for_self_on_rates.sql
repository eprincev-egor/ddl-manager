create or replace function cache_totals_for_self_on_rates()
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


    select
        new.quantity * new.price * calc_vat(
            new.vat_type,
            new.vat_value
        ) as total
    into new_totals;

    if new_totals.total is distinct from new.total then

        update rates set
            total = new_totals.total
        where
            public.rates.id = new.id;

    end if;

    return new;
end
$body$
language plpgsql;

create trigger cache_totals_for_self_on_rates
after update of price, quantity, vat_type, vat_value
on public.rates
for each row
execute procedure cache_totals_for_self_on_rates();