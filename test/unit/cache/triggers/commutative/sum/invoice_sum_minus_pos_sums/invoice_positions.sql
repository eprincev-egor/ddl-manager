create or replace function cache_balance_for_invoice_on_invoice_positions()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_invoice is not null
            and
            old.deleted = 0
        then
            update invoice set
                balance_sum = balance_sum - coalesce(
                    round(
                        old.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ),
                balance = invoice.invoice_summ - (balance_sum - coalesce(
                    round(
                        old.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ))
            where
                old.id_invoice = invoice.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.deleted is not distinct from old.deleted
            and
            new.id_invoice is not distinct from old.id_invoice
            and
            new.total_sum_with_vat_in_curs is not distinct from old.total_sum_with_vat_in_curs
        then
            return new;
        end if;

        if
            new.id_invoice is not distinct from old.id_invoice
            and
            new.deleted is not distinct from old.deleted
        then
            if
                new.id_invoice is null
                or
                not coalesce(new.deleted = 0, false)
            then
                return new;
            end if;

            update invoice set
                balance_sum = balance_sum - coalesce(
                    round(
                        old.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ) + coalesce(
                    round(
                        new.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ),
                balance = invoice.invoice_summ - (balance_sum - coalesce(
                    round(
                        old.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ) + coalesce(
                    round(
                        new.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ))
            where
                new.id_invoice = invoice.id;

            return new;
        end if;

        if
            old.id_invoice is not null
            and
            old.deleted = 0
        then
            update invoice set
                balance_sum = balance_sum - coalesce(
                    round(
                        old.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ),
                balance = invoice.invoice_summ - (balance_sum - coalesce(
                    round(
                        old.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ))
            where
                old.id_invoice = invoice.id;
        end if;

        if
            new.id_invoice is not null
            and
            new.deleted = 0
        then
            update invoice set
                balance_sum = balance_sum + coalesce(
                    round(
                        new.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ),
                balance = invoice.invoice_summ - (balance_sum + coalesce(
                    round(
                        new.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ))
            where
                new.id_invoice = invoice.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_invoice is not null
            and
            new.deleted = 0
        then
            update invoice set
                balance_sum = balance_sum + coalesce(
                    round(
                        new.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ),
                balance = invoice.invoice_summ - (balance_sum + coalesce(
                    round(
                        new.total_sum_with_vat_in_curs :: numeric,
                        - 2
                    ),
                    0
                ))
            where
                new.id_invoice = invoice.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_balance_for_invoice_on_invoice_positions
after insert or update of deleted, id_invoice, total_sum_with_vat_in_curs or delete
on public.invoice_positions
for each row
execute procedure cache_balance_for_invoice_on_invoice_positions();