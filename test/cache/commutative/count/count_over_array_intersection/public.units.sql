create or replace function cache_totals_for_gtd_on_units()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.orders_ids is not null then
            update gtd set
                units_count = units_count - 1
            where
                old.orders_ids && gtd.orders_ids;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.orders_ids is not distinct from old.orders_ids then
            return new;
        end if;



        if old.orders_ids is not null then
            update gtd set
                units_count = units_count - 1
            where
                old.orders_ids && gtd.orders_ids;
        end if;

        if new.orders_ids is not null then
            update gtd set
                units_count = units_count + 1
            where
                new.orders_ids && gtd.orders_ids;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.orders_ids is not null then
            update gtd set
                units_count = units_count + 1
            where
                new.orders_ids && gtd.orders_ids;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_gtd_on_units
after insert or update of orders_ids or delete
on public.units
for each row
execute procedure cache_totals_for_gtd_on_units();