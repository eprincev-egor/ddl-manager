create or replace function cache_renomination_for_self_on_invoice()
returns trigger as $body$
begin
    if TG_OP = 'INSERT' then
        if new.renomination_invoices is null then
            return new;
        end if;
    end if;

    if TG_OP = 'UPDATE' then
        if cm_equal_arrays(new.renomination_invoices, old.renomination_invoices) then
            return new;
        end if;
    end if;


    update invoice set
        (
            renomination_sum,
            renomination_link_account_no_doc_number,
            renomination_link,
            renomination_currencies_charcode,
            renomination_currencies
        ) = (
            select
                sum(renomination_invoice.sum) as renomination_sum,
                array_agg(
        renomination_invoice.account_no_doc_number
    ) as renomination_link_account_no_doc_number,
    string_agg(
        renomination_invoice.account_no_doc_number,
        ', '
    ) as renomination_link,
    array_agg(list_currency.charcode) as renomination_currencies_charcode,
    string_agg(distinct
        list_currency.charcode,
        ', '
    ) as renomination_currencies

            from invoice as renomination_invoice

            left join list_currency on
                list_currency.id = renomination_invoice.id_list_currency

            where
                renomination_invoice.id = any (invoice.renomination_invoices)
        )
    where
        public.invoice.id = new.id;


    return new;
end
$body$
language plpgsql;

create trigger cache_renomination_for_self_on_invoice
after insert or update of renomination_invoices
on public.invoice
for each row
execute procedure cache_renomination_for_self_on_invoice();