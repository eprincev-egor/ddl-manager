create or replace function cache_contract_for_invoice_on_list_contracts()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then
        if old.date_contract is not null then
            update invoice set
                date_contract = null
            where
                old.id = invoice.id_list_contracts;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if new.date_contract is not distinct from old.date_contract then
            return new;
        end if;

        update invoice set
            date_contract = new.date_contract
        where
            new.id = invoice.id_list_contracts;

        return new;
    end if;

    if TG_OP = 'INSERT' then
        if new.date_contract is not null then
            update invoice set
                date_contract = new.date_contract
            where
                new.id = invoice.id_list_contracts;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_contract_for_invoice_on_list_contracts
after insert or update of date_contract or delete
on public.list_contracts
for each row
execute procedure cache_contract_for_invoice_on_list_contracts();