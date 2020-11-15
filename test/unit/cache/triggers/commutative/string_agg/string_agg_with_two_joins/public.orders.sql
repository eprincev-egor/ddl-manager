create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare old_country_from_name text;
declare old_country_to_name text;
declare new_country_from_name text;
declare new_country_to_name text;
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then

            if old.id_country_from is not null then
                old_country_from_name = (
                    select
                        countries.name
                    from countries
                    where
                        countries.id = old.id_country_from
                );
            end if;

            if old.id_country_to is not null then
                old_country_to_name = (
                    select
                        countries.name
                    from countries
                    where
                        countries.id = old.id_country_to
                );
            end if;

            if
                old_country_from_name is not null
                or
                old_country_to_name is not null
            then
                update companies set
                    from_countries_array_agg = cm_array_remove_one_element(
                        from_countries_array_agg,
                        old_country_from_name
                    ),
                    from_countries = array_to_string(
                        cm_array_remove_one_element(
                            from_countries_array_agg,
                            old_country_from_name
                        ),
                        ', '
                    ),
                    to_countries_array_agg = cm_array_remove_one_element(
                        to_countries_array_agg,
                        old_country_to_name
                    ),
                    to_countries = array_to_string(
                        cm_array_remove_one_element(
                            to_countries_array_agg,
                            old_country_to_name
                        ),
                        ', '
                    )
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
            new.id_country_from is not distinct from old.id_country_from
            and
            new.id_country_to is not distinct from old.id_country_to
        then
            return new;
        end if;

        if old.id_country_from is not null then
            old_country_from_name = (
                select
                    countries.name
                from countries
                where
                    countries.id = old.id_country_from
            );
        end if;

        if old.id_country_to is not null then
            old_country_to_name = (
                select
                    countries.name
                from countries
                where
                    countries.id = old.id_country_to
            );
        end if;

        if new.id_country_from is not distinct from old.id_country_from then
            new_country_from_name = old_country_from_name;
        else
            if new.id_country_from is not null then
                new_country_from_name = (
                    select
                        countries.name
                    from countries
                    where
                        countries.id = new.id_country_from
                );
            end if;
        end if;

        if new.id_country_to is not distinct from old.id_country_to then
            new_country_to_name = old_country_to_name;
        else
            if new.id_country_to is not null then
                new_country_to_name = (
                    select
                        countries.name
                    from countries
                    where
                        countries.id = new.id_country_to
                );
            end if;
        end if;


        if new.id_client is not distinct from old.id_client then

            update companies set
                from_countries_array_agg = array_append(
                    cm_array_remove_one_element(
                        from_countries_array_agg,
                        old_country_from_name
                    ),
                    new_country_from_name
                ),
                from_countries = array_to_string(
                    array_append(
                        cm_array_remove_one_element(
                            from_countries_array_agg,
                            old_country_from_name
                        ),
                        new_country_from_name
                    ),
                    ', '
                ),
                to_countries_array_agg = array_append(
                    cm_array_remove_one_element(
                        to_countries_array_agg,
                        old_country_to_name
                    ),
                    new_country_to_name
                ),
                to_countries = array_to_string(
                    array_append(
                        cm_array_remove_one_element(
                            to_countries_array_agg,
                            old_country_to_name
                        ),
                        new_country_to_name
                    ),
                    ', '
                )
            where
                new.id_client = companies.id;

            return new;
        end if;


        if
            old.id_client is not null
            and
            (
                old_country_from_name is not null
                or
                old_country_to_name is not null
            )
        then
            update companies set
                from_countries_array_agg = cm_array_remove_one_element(
                    from_countries_array_agg,
                    old_country_from_name
                ),
                from_countries = array_to_string(
                    cm_array_remove_one_element(
                        from_countries_array_agg,
                        old_country_from_name
                    ),
                    ', '
                ),
                to_countries_array_agg = cm_array_remove_one_element(
                    to_countries_array_agg,
                    old_country_to_name
                ),
                to_countries = array_to_string(
                    cm_array_remove_one_element(
                        to_countries_array_agg,
                        old_country_to_name
                    ),
                    ', '
                )
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            (
                new_country_from_name is not null
                or
                new_country_to_name is not null
            )
        then
            update companies set
                from_countries_array_agg = array_append(
                    from_countries_array_agg,
                    new_country_from_name
                ),
                from_countries = array_to_string(
                    array_append(
                        from_countries_array_agg,
                        new_country_from_name
                    ),
                    ', '
                ),
                to_countries_array_agg = array_append(
                    to_countries_array_agg,
                    new_country_to_name
                ),
                to_countries = array_to_string(
                    array_append(
                        to_countries_array_agg,
                        new_country_to_name
                    ),
                    ', '
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then

            if new.id_country_from is not null then
                new_country_from_name = (
                    select
                        countries.name
                    from countries
                    where
                        countries.id = new.id_country_from
                );
            end if;

            if new.id_country_to is not null then
                new_country_to_name = (
                    select
                        countries.name
                    from countries
                    where
                        countries.id = new.id_country_to
                );
            end if;

            if
                new_country_from_name is not null
                or
                new_country_to_name is not null
            then
                update companies set
                    from_countries_array_agg = array_append(
                        from_countries_array_agg,
                        new_country_from_name
                    ),
                    from_countries = array_to_string(
                        array_append(
                            from_countries_array_agg,
                            new_country_from_name
                        ),
                        ', '
                    ),
                    to_countries_array_agg = array_append(
                        to_countries_array_agg,
                        new_country_to_name
                    ),
                    to_countries = array_to_string(
                        array_append(
                            to_countries_array_agg,
                            new_country_to_name
                        ),
                        ', '
                    )
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
after insert or update of id_client, id_country_from, id_country_to or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();