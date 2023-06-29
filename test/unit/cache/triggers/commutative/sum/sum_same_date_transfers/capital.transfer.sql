create or replace function cache_totals_for_account_date_balance_on_transfer()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if old.transfer_date is not null then
            update capital.account_date_balance as date_balance set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    transfers_sum
                ) = (
                    select
                            sum(source_row.delta) as transfers_sum
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::capital.transfer, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.transfer_date = date_balance.balance_date
                )
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
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'delta', new.delta,'id', new.id,'transfer_date', new.transfer_date
            ),
            TG_OP
        ),
                (
                    transfers_sum
                ) = (
                    select
                            sum(source_row.delta) as transfers_sum
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'delta', new.delta,'id', new.id,'transfer_date', new.transfer_date
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::capital.transfer, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.transfer_date = date_balance.balance_date
                )
            where
                new.transfer_date = date_balance.balance_date;

            return new;
        end if;

        if old.transfer_date is not null then
            update capital.account_date_balance as date_balance set
                __totals_json__ = __totals_json__ - old.id::text,
                (
                    transfers_sum
                ) = (
                    select
                            sum(source_row.delta) as transfers_sum
                    from (
                        select
                                record.*
                        from jsonb_each(
    __totals_json__ - old.id::text
) as json_entry

                        left join lateral jsonb_populate_record(null::capital.transfer, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.transfer_date = date_balance.balance_date
                )
            where
                old.transfer_date = date_balance.balance_date;
        end if;

        if new.transfer_date is not null then
            update capital.account_date_balance as date_balance set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'delta', new.delta,'id', new.id,'transfer_date', new.transfer_date
            ),
            TG_OP
        ),
                (
                    transfers_sum
                ) = (
                    select
                            sum(source_row.delta) as transfers_sum
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'delta', new.delta,'id', new.id,'transfer_date', new.transfer_date
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::capital.transfer, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.transfer_date = date_balance.balance_date
                )
            where
                new.transfer_date = date_balance.balance_date;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if new.transfer_date is not null then
            update capital.account_date_balance as date_balance set
                __totals_json__ = cm_merge_json(
            __totals_json__,
            null::jsonb,
            jsonb_build_object(
                'delta', new.delta,'id', new.id,'transfer_date', new.transfer_date
            ),
            TG_OP
        ),
                (
                    transfers_sum
                ) = (
                    select
                            sum(source_row.delta) as transfers_sum
                    from (
                        select
                                record.*
                        from jsonb_each(
    cm_merge_json(
                __totals_json__,
                null::jsonb,
                jsonb_build_object(
                    'delta', new.delta,'id', new.id,'transfer_date', new.transfer_date
                ),
                TG_OP
            )
) as json_entry

                        left join lateral jsonb_populate_record(null::capital.transfer, json_entry.value) as record on
                            true
                    ) as source_row
                    where
                        source_row.transfer_date = date_balance.balance_date
                )
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