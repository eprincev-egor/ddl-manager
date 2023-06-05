create or replace function cache000_contract_for_invoice_bef_upd()
returns trigger as $body$
declare new_totals record;
begin

    if new.id_list_contracts is not distinct from old.id_list_contracts then
        return new;
    end if;


    select
        list_contracts.date_contract as date_contract
    from list_contracts
    where
        list_contracts.id = new.id_list_contracts
    into new_totals;


    new.date_contract = new_totals.date_contract;


    return new;
end
$body$
language plpgsql;

create trigger cache000_contract_for_invoice_bef_upd
before update of id_list_contracts
on public.invoice
for each row
execute procedure cache000_contract_for_invoice_bef_upd();