create or replace function cache_totals_for_self_on_log_oper()
returns trigger as $body$
declare new_totals record;
begin

    if TG_OP = 'UPDATE' then
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
    end if;


    select
        calc_vat(
            new.buy_vat_type,
            new.buy_vat_value
        ) as bay_vat,
        calc_vat(
            new.sale_vat_type,
            new.sale_vat_value
        ) as sale_vat
    into new_totals;

    if
        new_totals.bay_vat is distinct from new.bay_vat
        or
        new_totals.sale_vat is distinct from new.sale_vat
    then

        update log_oper set
            bay_vat = new_totals.bay_vat,
            sale_vat = new_totals.sale_vat
        where
            public.log_oper.id = new.id;

    end if;

    return new;
end
$body$
language plpgsql;

create trigger cache_totals_for_self_on_log_oper
after insert or update of buy_vat_type, buy_vat_value, sale_vat_type, sale_vat_value
on public.log_oper
for each row
execute procedure cache_totals_for_self_on_log_oper();