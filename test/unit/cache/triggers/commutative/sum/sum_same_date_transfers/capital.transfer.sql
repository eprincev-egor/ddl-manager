create or replace function cache_totals_for_account_date_balance_on_transfer()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.transfer_date is not null then
            update capital.account_date_balance as date_balance set
                transfers_sum = coalesce(transfers_sum, 0) - coalesce(old.delta, 0)
            where
                old.transfer_date = date_balance.balance_date;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.delta is not distinct from old.delta
            and
            new.transfer_date is not distinct from old.transfer_date
        then
            return new;
        end if;

        if new.transfer_date is not distinct from old.transfer_date then
            if new.transfer_date is null then
                return new;
            end if;

            update capital.account_date_balance as date_balance set
                transfers_sum = coalesce(transfers_sum, 0) - coalesce(old.delta, 0) + coalesce(new.delta, 0)
            where
                new.transfer_date = date_balance.balance_date;

            return new;
        end if;

        if old.transfer_date is not null then
            update capital.account_date_balance as date_balance set
                transfers_sum = coalesce(transfers_sum, 0) - coalesce(old.delta, 0)
            where
                old.transfer_date = date_balance.balance_date;
        end if;

        if new.transfer_date is not null then
            update capital.account_date_balance as date_balance set
                transfers_sum = coalesce(transfers_sum, 0) + coalesce(new.delta, 0)
            where
                new.transfer_date = date_balance.balance_date;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.transfer_date is not null then
            update capital.account_date_balance as date_balance set
                transfers_sum = coalesce(transfers_sum, 0) + coalesce(new.delta, 0)
            where
                new.transfer_date = date_balance.balance_date;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_account_date_balance_on_transfer
after insert or update of delta, transfer_date or delete
on capital.transfer
for each row
execute procedure cache_totals_for_account_date_balance_on_transfer();