create or replace function cache_orders_for_orders_positions_on_orders()
returns trigger as $body$
declare country_start_row record;
begin

    if TG_OP = 'DELETE' then
        if old.deleted = 0 then
            update orders_positions as positions set
                has_surveyor_inspection = coalesce(null, 0)
            where
                old.id = positions.id_supply_order
                and
                positions.has_surveyor_inspection is distinct from (coalesce(null, 0));
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_country_start is not distinct from old.id_country_start
        then
            return new;
        end if;

        if
            coalesce(old.deleted = 0, false)
            or
            coalesce(new.deleted = 0, false)
        then

            if new.id_country_start is not null then
                select
                    countries.has_surveyor_inspection
                into
                    country_start_row
                from countries
                where
                    public.countries.id = new.id_country_start;
            end if;

            update orders_positions as positions set
                has_surveyor_inspection = case
                    when
                        coalesce(new.deleted = 0, false)
                    then
                        coalesce(
                            country_start_row.has_surveyor_inspection,
                            0
                        )
                    else
                        null
                end
            where
                new.id = positions.id_supply_order
                and
                positions.has_surveyor_inspection is distinct from (case
                    when
                        coalesce(new.deleted = 0, false)
                    then
                        coalesce(
                            country_start_row.has_surveyor_inspection,
                            0
                        )
                    else
                        null
                end);
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if new.deleted = 0 then

            if new.id_country_start is not null then
                select
                    countries.has_surveyor_inspection
                into
                    country_start_row
                from countries
                where
                    public.countries.id = new.id_country_start;
            end if;

            update orders_positions as positions set
                has_surveyor_inspection = coalesce(
                    country_start_row.has_surveyor_inspection,
                    0
                )
            where
                new.id = positions.id_supply_order
                and
                positions.has_surveyor_inspection is distinct from (coalesce(
                    country_start_row.has_surveyor_inspection,
                    0
                ));
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_orders_for_orders_positions_on_orders
after insert or update of deleted, id_country_start or delete
on public.orders
for each row
execute procedure cache_orders_for_orders_positions_on_orders();