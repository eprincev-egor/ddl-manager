create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare old_country_code text;
declare old_country_name text;
declare new_country_code text;
declare new_country_name text;
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            if old.id_country is not null then
                select
                    countries.code,
                    countries.name
                into
                    old_country_code,
                    old_country_name
                from countries
                where
                    countries.id = old.id_country;
            end if;

            update companies set
                countries_codes_code = cm_array_remove_one_element(
                    countries_codes_code,
                    old_country_code
                ),
                countries_codes = (
                    select
                        string_agg(item.code, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            countries_codes_code,
                            old_country_code
                        )
                    ) as item(code)
                ),
                countries_names_name = cm_array_remove_one_element(
                    countries_names_name,
                    old_country_name
                ),
                countries_names = (
                    select
                        string_agg(item.name, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            countries_names_name,
                            old_country_name
                        )
                    ) as item(name)
                )
            where
                old.id_client = companies.id;
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
            select
                countries.code,
                countries.name
            into
                old_country_code,
                old_country_name
            from countries
            where
                countries.id = old.id_country;
        end if;

        if new.id_country is not distinct from old.id_country then
            new_country_code = old_country_code;
            new_country_name = old_country_name;
        else
            if new.id_country is not null then
                select
                    countries.code,
                    countries.name
                into
                    new_country_code,
                    new_country_name
                from countries
                where
                    countries.id = new.id_country;
            end if;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                countries_codes_code = array_append(
                    cm_array_remove_one_element(
                        countries_codes_code,
                        old_country_code
                    ),
                    new_country_code
                ),
                countries_codes = (
                    select
                        string_agg(item.code, ', ')

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                countries_codes_code,
                                old_country_code
                            ),
                            new_country_code
                        )
                    ) as item(code)
                ),
                countries_names_name = array_append(
                    cm_array_remove_one_element(
                        countries_names_name,
                        old_country_name
                    ),
                    new_country_name
                ),
                countries_names = (
                    select
                        string_agg(item.name, ', ')

                    from unnest(
                        array_append(
                            cm_array_remove_one_element(
                                countries_names_name,
                                old_country_name
                            ),
                            new_country_name
                        )
                    ) as item(name)
                )
            where
                new.id_client = companies.id;

            return new;
        end if;

        if old.id_client is not null then
            update companies set
                countries_codes_code = cm_array_remove_one_element(
                    countries_codes_code,
                    old_country_code
                ),
                countries_codes = (
                    select
                        string_agg(item.code, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            countries_codes_code,
                            old_country_code
                        )
                    ) as item(code)
                ),
                countries_names_name = cm_array_remove_one_element(
                    countries_names_name,
                    old_country_name
                ),
                countries_names = (
                    select
                        string_agg(item.name, ', ')

                    from unnest(
                        cm_array_remove_one_element(
                            countries_names_name,
                            old_country_name
                        )
                    ) as item(name)
                )
            where
                old.id_client = companies.id;
        end if;

        if new.id_client is not null then
            update companies set
                countries_codes_code = array_append(
                    countries_codes_code,
                    new_country_code
                ),
                countries_codes = coalesce(
                    countries_codes ||
                    coalesce(
                        ', '
                        || new_country_code,
                        ''
                    ),
                    new_country_code
                ),
                countries_names_name = array_append(
                    countries_names_name,
                    new_country_name
                ),
                countries_names = coalesce(
                    countries_names ||
                    coalesce(
                        ', '
                        || new_country_name,
                        ''
                    ),
                    new_country_name
                )
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            if new.id_country is not null then
                select
                    countries.code,
                    countries.name
                into
                    new_country_code,
                    new_country_name
                from countries
                where
                    countries.id = new.id_country;
            end if;

            update companies set
                countries_codes_code = array_append(
                    countries_codes_code,
                    new_country_code
                ),
                countries_codes = coalesce(
                    countries_codes ||
                    coalesce(
                        ', '
                        || new_country_code,
                        ''
                    ),
                    new_country_code
                ),
                countries_names_name = array_append(
                    countries_names_name,
                    new_country_name
                ),
                countries_names = coalesce(
                    countries_names ||
                    coalesce(
                        ', '
                        || new_country_name,
                        ''
                    ),
                    new_country_name
                )
            where
                new.id_client = companies.id;
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