create or replace function cache_totals_for_companies_on_companies()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_parent_company is not null then
            update companies as main_company set
                child_companies_ids = cm_array_remove_one_element(
                    child_companies_ids,
                    old.id
                )
            where
                old.id_parent_company = main_company.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.id_parent_company is not distinct from old.id_parent_company then
            return new;
        end if;



        if old.id_parent_company is not null then
            update companies as main_company set
                child_companies_ids = cm_array_remove_one_element(
                    child_companies_ids,
                    old.id
                )
            where
                old.id_parent_company = main_company.id;
        end if;

        if new.id_parent_company is not null then
            update companies as main_company set
                child_companies_ids = array_append(
                    child_companies_ids,
                    new.id
                )
            where
                new.id_parent_company = main_company.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_parent_company is not null then
            update companies as main_company set
                child_companies_ids = array_append(
                    child_companies_ids,
                    new.id
                )
            where
                new.id_parent_company = main_company.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_companies
after insert or update of id_parent_company or delete
on public.companies
for each row
execute procedure cache_totals_for_companies_on_companies();