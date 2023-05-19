create or replace function cache_contract_for_self_on_invoice()
returns trigger as $body$
begin

    if new.id_list_contracts is not distinct from old.id_list_contracts then
        return new;
    end if;


    update invoice set
        (
            date_contract
        ) = (
            select
                list_contracts.date_contract as date_contract

            from list_contracts

            where
                list_contracts.id = invoice.id_list_contracts
        )
    where
        public.invoice.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_contract_for_self_on_invoice
after update of id_list_contracts
on public.invoice
for each row
execute procedure cache_contract_for_self_on_invoice();