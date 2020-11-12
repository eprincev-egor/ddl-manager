create or replace function cache_totals_for_companies_on_orders()
returns trigger as $body$
begin

    if TG_OP = 'DELETE' then

        if
            old.id_client is not null
            and
            coalesce(old.total, 0) != 0
            and
            (
                old.is_sale
                or
                old.is_buy
            )
        then
            update companies set
                orders_profit_sum_total_is_sale = case
                    when
                        old.is_sale
                    then
                        orders_profit_sum_total_is_sale - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_sale
                end,
                orders_profit_sum_total_is_buy = case
                    when
                        old.is_buy
                    then
                        orders_profit_sum_total_is_buy - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_buy
                end,
                orders_profit = (case
                    when
                        old.is_sale
                    then
                        orders_profit_sum_total_is_sale - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_sale
                end) - (case
                    when
                        old.is_buy
                    then
                        orders_profit_sum_total_is_buy - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_buy
                end)
            where
                old.id_client = companies.id;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.id_client is not distinct from old.id_client
            and
            new.is_buy is not distinct from old.is_buy
            and
            new.is_sale is not distinct from old.is_sale
            and
            new.total is not distinct from old.total
        then
            return new;
        end if;

        if new.id_client is not distinct from old.id_client then
            update companies set
                orders_profit_sum_total_is_sale = case
                    when
                        new.is_sale
                        and
                        not(old.is_sale)
                    then
                        orders_profit_sum_total_is_sale + coalesce(new.total, 0)
                    when
                        not(new.is_sale)
                        and
                        old.is_sale
                    then
                        orders_profit_sum_total_is_sale - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_sale - coalesce(old.total, 0) + coalesce(new.total, 0)
                end,
                orders_profit_sum_total_is_buy = case
                    when
                        new.is_buy
                        and
                        not(old.is_buy)
                    then
                        orders_profit_sum_total_is_buy + coalesce(new.total, 0)
                    when
                        not(new.is_buy)
                        and
                        old.is_buy
                    then
                        orders_profit_sum_total_is_buy - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_buy - coalesce(old.total, 0) + coalesce(new.total, 0)
                end,
                orders_profit = (case
                    when
                        new.is_sale
                        and
                        not(old.is_sale)
                    then
                        orders_profit_sum_total_is_sale + coalesce(new.total, 0)
                    when
                        not(new.is_sale)
                        and
                        old.is_sale
                    then
                        orders_profit_sum_total_is_sale - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_sale - coalesce(old.total, 0) + coalesce(new.total, 0)
                end) - (case
                    when
                        new.is_buy
                        and
                        not(old.is_buy)
                    then
                        orders_profit_sum_total_is_buy + coalesce(new.total, 0)
                    when
                        not(new.is_buy)
                        and
                        old.is_buy
                    then
                        orders_profit_sum_total_is_buy - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_buy - coalesce(old.total, 0) + coalesce(new.total, 0)
                end)
            where
                new.id_client = companies.id;

            return new;
        end if;

        if
            old.id_client is not null
            and
            coalesce(old.total, 0) != 0
            and
            (
                old.is_sale
                or
                old.is_buy
            )
        then
            update companies set
                orders_profit_sum_total_is_sale = case
                    when
                        old.is_sale
                    then
                        orders_profit_sum_total_is_sale - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_sale
                end,
                orders_profit_sum_total_is_buy = case
                    when
                        old.is_buy
                    then
                        orders_profit_sum_total_is_buy - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_buy
                end,
                orders_profit = (case
                    when
                        old.is_sale
                    then
                        orders_profit_sum_total_is_sale - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_sale
                end) - (case
                    when
                        old.is_buy
                    then
                        orders_profit_sum_total_is_buy - coalesce(old.total, 0)
                    else
                        orders_profit_sum_total_is_buy
                end)
            where
                old.id_client = companies.id;
        end if;

        if
            new.id_client is not null
            and
            coalesce(new.total, 0) != 0
            and
            (
                new.is_sale
                or
                new.is_buy
            )
        then
            update companies set
                orders_profit_sum_total_is_sale = case
                    when
                        new.is_sale
                    then
                        orders_profit_sum_total_is_sale + coalesce(new.total, 0)
                    else
                        orders_profit_sum_total_is_sale
                end,
                orders_profit_sum_total_is_buy = case
                    when
                        new.is_buy
                    then
                        orders_profit_sum_total_is_buy + coalesce(new.total, 0)
                    else
                        orders_profit_sum_total_is_buy
                end,
                orders_profit = (case
                    when
                        new.is_sale
                    then
                        orders_profit_sum_total_is_sale + coalesce(new.total, 0)
                    else
                        orders_profit_sum_total_is_sale
                end) - (case
                    when
                        new.is_buy
                    then
                        orders_profit_sum_total_is_buy + coalesce(new.total, 0)
                    else
                        orders_profit_sum_total_is_buy
                end)
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.id_client is not null
            and
            coalesce(new.total, 0) != 0
            and
            (
                new.is_sale
                or
                new.is_buy
            )
        then
            update companies set
                orders_profit_sum_total_is_sale = case
                    when
                        new.is_sale
                    then
                        orders_profit_sum_total_is_sale + coalesce(new.total, 0)
                    else
                        orders_profit_sum_total_is_sale
                end,
                orders_profit_sum_total_is_buy = case
                    when
                        new.is_buy
                    then
                        orders_profit_sum_total_is_buy + coalesce(new.total, 0)
                    else
                        orders_profit_sum_total_is_buy
                end,
                orders_profit = (case
                    when
                        new.is_sale
                    then
                        orders_profit_sum_total_is_sale + coalesce(new.total, 0)
                    else
                        orders_profit_sum_total_is_sale
                end) - (case
                    when
                        new.is_buy
                    then
                        orders_profit_sum_total_is_buy + coalesce(new.total, 0)
                    else
                        orders_profit_sum_total_is_buy
                end)
            where
                new.id_client = companies.id;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_totals_for_companies_on_orders
after insert or update of id_client, is_buy, is_sale, total or delete
on public.orders
for each row
execute procedure cache_totals_for_companies_on_orders();