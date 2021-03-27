create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
declare old_country_name text;
declare new_country_name text;
declare matched_old boolean;
declare matched_new boolean;
declare inserted_clients_ids integer[];
declare not_changed_clients_ids integer[];
declare deleted_clients_ids integer[];
begin

    if TG_OP = 'DELETE' then

        if
            old.clients_ids is not null
            and
            old.deleted = 0
        then
            if old.id_country is not null then
                old_country_name = (
                    select
                        countries.name
                    from countries
                    where
                        countries.id = old.id_country
                );
            end if;

            update companies set
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
                companies.id = any( old.clients_ids );
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            cm_equal_arrays(new.clients_ids, old.clients_ids)
            and
            new.deleted is not distinct from old.deleted
            and
            new.id_country is not distinct from old.id_country
        then
            return new;
        end if;

        matched_old = coalesce(old.deleted = 0, false);
        matched_new = coalesce(new.deleted = 0, false);

        if
            not matched_old
            and
            not matched_new
        then
            return new;
        end if;

        if
            matched_old
            and
            not matched_new
        then
            inserted_clients_ids = null;
            not_changed_clients_ids = null;
            deleted_clients_ids = old.clients_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_clients_ids = new.clients_ids;
            not_changed_clients_ids = null;
            deleted_clients_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_clients_ids = cm_get_inserted_elements(old.clients_ids, new.clients_ids);
            not_changed_clients_ids = cm_get_not_changed_elements(old.clients_ids, new.clients_ids);
            deleted_clients_ids = cm_get_deleted_elements(old.clients_ids, new.clients_ids);
        end if;

        if old.id_country is not null then
            old_country_name = (
                select
                    countries.name
                from countries
                where
                    countries.id = old.id_country
            );
        end if;

        if new.id_country is not distinct from old.id_country then
            new_country_name = old_country_name;
        else
            if new.id_country is not null then
                new_country_name = (
                    select
                        countries.name
                    from countries
                    where
                        countries.id = new.id_country
                );
            end if;
        end if;

        if not_changed_clients_ids is not null then
            update companies set
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
                companies.id = any( not_changed_clients_ids );
        end if;

        if deleted_clients_ids is not null then
            update companies set
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
                companies.id = any( deleted_clients_ids );
        end if;

        if inserted_clients_ids is not null then
            update companies set
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
                companies.id = any( inserted_clients_ids );
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.clients_ids is not null
            and
            new.deleted = 0
        then
            if new.id_country is not null then
                new_country_name = (
                    select
                        countries.name
                    from countries
                    where
                        countries.id = new.id_country
                );
            end if;

            update companies set
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
                companies.id = any( new.clients_ids );
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of clients_ids, deleted, id_country or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();