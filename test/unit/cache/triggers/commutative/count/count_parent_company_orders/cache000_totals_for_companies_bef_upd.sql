create or replace function cache000_totals_for_companies_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if new.id_parent_company is not distinct from old.id_parent_company then
        return new;
    end if;


    select
            count(*) as parent_company_orders_count,
            ('{' || string_agg(
                                        '"' || public.orders.id::text || '":' || jsonb_build_object(
                            'id', public.orders.id,'id_client', public.orders.id_client
                        )::text,
                                        ','
                                    ) || '}')
            ::
            jsonb as __totals_json__
    from orders
    where
        orders.id_client = new.id_parent_company
    into new_totals;


    new.parent_company_orders_count = new_totals.parent_company_orders_count;
    new.__totals_json__ = new_totals.__totals_json__;


    return new;
end
$body$
language plpgsql;

create trigger cache000_totals_for_companies_bef_upd
before update of id_parent_company
on public.companies
for each row
execute procedure cache000_totals_for_companies_bef_upd();