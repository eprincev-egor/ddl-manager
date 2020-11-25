create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.id_client is not null then
            update companies as child_company set
                parent_company_orders_count = parent_company_orders_count - 1
            where
                old.id_client = child_company.id_parent_company;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.id_client is not distinct from old.id_client then
            return new;
        end if;



        if old.id_client is not null then
            update companies as child_company set
                parent_company_orders_count = parent_company_orders_count - 1
            where
                old.id_client = child_company.id_parent_company;
        end if;

        if new.id_client is not null then
            update companies as child_company set
                parent_company_orders_count = parent_company_orders_count + 1
            where
                new.id_client = child_company.id_parent_company;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.id_client is not null then
            update companies as child_company set
                parent_company_orders_count = parent_company_orders_count + 1
            where
                new.id_client = child_company.id_parent_company;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();