create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare old_country_code text;
declare new_country_code text;
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            if old.id_country is not null then
                old_country_code = (
                    select
                        countries.code
                    from countries
                    where
                        countries.id = old.id_country
                );
            end if;

            if
                coalesce(old_country_code = 'RUS', false)
                or
                coalesce(old_country_code = 'ENG', false)
            then
                update companies set
                    rus_orders_count = case
                        when
                            old_country_code = 'RUS'
                        then
                            rus_orders_count - 1
                        else
                            rus_orders_count
                    end,
                    eng_orders_count = case
                        when
                            old_country_code = 'ENG'
                        then
                            eng_orders_count - 1
                        else
                            eng_orders_count
                    end
                where
                    old.id_client = companies.id;
            end if;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.id_country is not distinct from old.id_country
        then
            return new;
        end if;

        if old.id_country is not null then
            old_country_code = (
                select
                    countries.code
                from countries
                where
                    countries.id = old.id_country
            );
        end if;

        if new.id_country is not distinct from old.id_country then
            new_country_code = old_country_code;
        else
            if new.id_country is not null then
                new_country_code = (
                    select
                        countries.code
                    from countries
                    where
                        countries.id = new.id_country
                );
            end if;
        end if;

        if new.id_client is not distinct from old.id_client then
            if new.id_client is null then
                return new;
            end if;

            update companies set
                rus_orders_count = case
                    when
                        new_country_code = 'RUS'
                        and
                        not coalesce(old_country_code = 'RUS', false)
                    then
                        rus_orders_count + 1
                    when
                        not coalesce(new_country_code = 'RUS', false)
                        and
                        old_country_code = 'RUS'
                    then
                        rus_orders_count - 1
                    else
                        rus_orders_count
                end,
                eng_orders_count = case
                    when
                        new_country_code = 'ENG'
                        and
                        not coalesce(old_country_code = 'ENG', false)
                    then
                        eng_orders_count + 1
                    when
                        not coalesce(new_country_code = 'ENG', false)
                        and
                        old_country_code = 'ENG'
                    then
                        eng_orders_count - 1
                    else
                        eng_orders_count
                end
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            (
                coalesce(old_country_code = 'RUS', false)
                or
                coalesce(old_country_code = 'ENG', false)
            )
        then
            update companies set
                rus_orders_count = case
                    when
                        old_country_code = 'RUS'
                    then
                        rus_orders_count - 1
                    else
                        rus_orders_count
                end,
                eng_orders_count = case
                    when
                        old_country_code = 'ENG'
                    then
                        eng_orders_count - 1
                    else
                        eng_orders_count
                end
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            (
                coalesce(new_country_code = 'RUS', false)
                or
                coalesce(new_country_code = 'ENG', false)
            )
        then
            update companies set
                rus_orders_count = case
                    when
                        new_country_code = 'RUS'
                    then
                        rus_orders_count + 1
                    else
                        rus_orders_count
                end,
                eng_orders_count = case
                    when
                        new_country_code = 'ENG'
                    then
                        eng_orders_count + 1
                    else
                        eng_orders_count
                end
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            if new.id_country is not null then
                new_country_code = (
                    select
                        countries.code
                    from countries
                    where
                        countries.id = new.id_country
                );
            end if;

            if
                coalesce(new_country_code = 'RUS', false)
                or
                coalesce(new_country_code = 'ENG', false)
            then
                update companies set
                    rus_orders_count = case
                        when
                            new_country_code = 'RUS'
                        then
                            rus_orders_count + 1
                        else
                            rus_orders_count
                    end,
                    eng_orders_count = case
                        when
                            new_country_code = 'ENG'
                        then
                            eng_orders_count + 1
                        else
                            eng_orders_count
                    end
                where
                    new.id_client = companies.id;
            end if;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client, id_country or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();