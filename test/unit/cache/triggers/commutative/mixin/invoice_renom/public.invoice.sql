create or replace function cache_renomination_for_invoice_on_invoice()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        update invoice set
            __renomination_json__ = __renomination_json__ - old.id::text,
            (
                renomination_sum,
                renomination_link,
                renomination_currencies
            ) = (
                select
                        sum(source_row.sum) as renomination_sum,
                        string_agg(
                            source_row.account_no_doc_number,
                            ', '
                                                ) as renomination_link,
                        string_agg(distinct 
                            list_currency.charcode,
                            ', '
                                                ) as renomination_currencies
                from (
                    select
                            record.*
                    from jsonb_each(
    __renomination_json__ - old.id::text
) as json_entry

                    left join lateral jsonb_populate_record(null::public.invoice, json_entry.value) as record on
                        true
                ) as source_row

                left join list_currency on
                    list_currency.id = source_row.id_list_currency
                where
                    source_row.id = any (invoice.renomination_invoices)
            )
        where
            invoice.renomination_invoices && ARRAY[ old.id ]::int8[];

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.account_no_doc_number is not distinct from old.account_no_doc_number
            and
            new.id_list_currency is not distinct from old.id_list_currency
            and
            new.sum is not distinct from old.sum
        then
            return new;
        end if;

        update invoice set
            __renomination_json__ = cm_merge_json(
            __renomination_json__,
            null::jsonb,
            jsonb_build_object(
            'account_no_doc_number', new.account_no_doc_number,'id', new.id,'id_list_currency', new.id_list_currency,'sum', new.sum
        ),
            TG_OP
        ),
            (
                renomination_sum,
                renomination_link,
                renomination_currencies
            ) = (
                select
                        sum(source_row.sum) as renomination_sum,
                        string_agg(
                            source_row.account_no_doc_number,
                            ', '
                                                ) as renomination_link,
                        string_agg(distinct 
                            list_currency.charcode,
                            ', '
                                                ) as renomination_currencies
                from (
                    select
                            record.*
                    from jsonb_each(
    cm_merge_json(
                __renomination_json__,
                null::jsonb,
                jsonb_build_object(
                'account_no_doc_number', new.account_no_doc_number,'id', new.id,'id_list_currency', new.id_list_currency,'sum', new.sum
            ),
                TG_OP
            )
) as json_entry

                    left join lateral jsonb_populate_record(null::public.invoice, json_entry.value) as record on
                        true
                ) as source_row

                left join list_currency on
                    list_currency.id = source_row.id_list_currency
                where
                    source_row.id = any (invoice.renomination_invoices)
            )
        where
            invoice.renomination_invoices && ARRAY[ new.id ]::int8[];



        return new;
    end if;
end
$body$
language plpgsql;

create trigger cache_renomination_for_invoice_on_invoice
after update of account_no_doc_number, id_list_currency, sum or delete
on public.invoice
for each row
execute procedure cache_renomination_for_invoice_on_invoice();