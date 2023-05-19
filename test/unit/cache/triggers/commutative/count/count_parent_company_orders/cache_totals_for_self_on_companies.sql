create or replace function cache_totals_for_self_on_companies()
returns trigger as $body$
begin

    if new.id_parent_company is not distinct from old.id_parent_company then
        return new;
    end if;


    update companies as child_company set
        (
            parent_company_orders_count
        ) = (
            select
                count(*) as parent_company_orders_count

            from orders

            where
                orders.id_client = child_company.id_parent_company
        )
    where
        child_company.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_totals_for_self_on_companies
after update of id_parent_company
on public.companies
for each row
execute procedure cache_totals_for_self_on_companies();