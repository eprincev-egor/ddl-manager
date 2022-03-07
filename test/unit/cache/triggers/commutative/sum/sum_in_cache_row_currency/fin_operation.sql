create or replace function cache_fin_totals_for_owner_unit_on_fin_operation()
returns trigger as $body$
declare matched_old boolean;
declare matched_new boolean;
declare inserted_units_ids bigint[];
declare not_changed_units_ids bigint[];
declare deleted_units_ids bigint[];
begin

    if TG_OP = 'DELETE' then

        if
            old.units_ids is not null
            and
            old.deleted = 0
        then
            update operation.owner_unit as own_unit set
                fin_sum = coalesce(fin_sum, 0) - coalesce(
                    round(
                        coalesce(old.sum_vat, 0 :: bigint) :: numeric * get_curs(
                            old.id_currency,
                            own_unit.id_currency_fin_oper,
                            old.curs,
                            coalesce(old.doc_date, now_utc()),
                            old.is_euro_zone_curs
                        ) :: numeric,
                        - 2
                    ) :: bigint / array_length(old.units_ids, 1),
                    0
                )
            where
                old.units_ids && own_unit.units_ids;
        end if;

        return old;
    end if;

    if TG_OP = 'UPDATE' then
        if
            new.curs is not distinct from old.curs
            and
            new.deleted is not distinct from old.deleted
            and
            new.doc_date is not distinct from old.doc_date
            and
            new.id_currency is not distinct from old.id_currency
            and
            new.is_euro_zone_curs is not distinct from old.is_euro_zone_curs
            and
            new.sum_vat is not distinct from old.sum_vat
            and
            new.units_ids is not distinct from old.units_ids
        then
            return new;
        end if;

        matched_old = coalesce(old.deleted = 0, false);
        matched_new = coalesce(new.deleted = 0, false);

        if
            not matched_old
            and
            not matched_new
        then
            return new;
        end if;

        if
            matched_old
            and
            not matched_new
        then
            inserted_units_ids = null;
            not_changed_units_ids = null;
            deleted_units_ids = old.units_ids;
        end if;

        if
            not matched_old
            and
            matched_new
        then
            inserted_units_ids = new.units_ids;
            not_changed_units_ids = null;
            deleted_units_ids = null;
        end if;

        if
            matched_old
            and
            matched_new
        then
            inserted_units_ids = cm_get_inserted_elements(old.units_ids, new.units_ids);
            not_changed_units_ids = cm_get_not_changed_elements(old.units_ids, new.units_ids);
            deleted_units_ids = cm_get_deleted_elements(old.units_ids, new.units_ids);
        end if;

        if not_changed_units_ids is not null then
            update operation.owner_unit as own_unit set
                fin_sum = coalesce(fin_sum, 0) - coalesce(
                    round(
                        coalesce(old.sum_vat, 0 :: bigint) :: numeric * get_curs(
                            old.id_currency,
                            own_unit.id_currency_fin_oper,
                            old.curs,
                            coalesce(old.doc_date, now_utc()),
                            old.is_euro_zone_curs
                        ) :: numeric,
                        - 2
                    ) :: bigint / array_length(old.units_ids, 1),
                    0
                ) + coalesce(
                    round(
                        coalesce(new.sum_vat, 0 :: bigint) :: numeric * get_curs(
                            new.id_currency,
                            own_unit.id_currency_fin_oper,
                            new.curs,
                            coalesce(new.doc_date, now_utc()),
                            new.is_euro_zone_curs
                        ) :: numeric,
                        - 2
                    ) :: bigint / array_length(new.units_ids, 1),
                    0
                )
            where
                not_changed_units_ids && own_unit.units_ids;
        end if;

        if deleted_units_ids is not null then
            update operation.owner_unit as own_unit set
                fin_sum = coalesce(fin_sum, 0) - coalesce(
                    round(
                        coalesce(old.sum_vat, 0 :: bigint) :: numeric * get_curs(
                            old.id_currency,
                            own_unit.id_currency_fin_oper,
                            old.curs,
                            coalesce(old.doc_date, now_utc()),
                            old.is_euro_zone_curs
                        ) :: numeric,
                        - 2
                    ) :: bigint / array_length(old.units_ids, 1),
                    0
                )
            where
                deleted_units_ids && own_unit.units_ids;
        end if;

        if inserted_units_ids is not null then
            update operation.owner_unit as own_unit set
                fin_sum = coalesce(fin_sum, 0) + coalesce(
                    round(
                        coalesce(new.sum_vat, 0 :: bigint) :: numeric * get_curs(
                            new.id_currency,
                            own_unit.id_currency_fin_oper,
                            new.curs,
                            coalesce(new.doc_date, now_utc()),
                            new.is_euro_zone_curs
                        ) :: numeric,
                        - 2
                    ) :: bigint / array_length(new.units_ids, 1),
                    0
                )
            where
                inserted_units_ids && own_unit.units_ids;
        end if;

        return new;
    end if;

    if TG_OP = 'INSERT' then

        if
            new.units_ids is not null
            and
            new.deleted = 0
        then
            update operation.owner_unit as own_unit set
                fin_sum = coalesce(fin_sum, 0) + coalesce(
                    round(
                        coalesce(new.sum_vat, 0 :: bigint) :: numeric * get_curs(
                            new.id_currency,
                            own_unit.id_currency_fin_oper,
                            new.curs,
                            coalesce(new.doc_date, now_utc()),
                            new.is_euro_zone_curs
                        ) :: numeric,
                        - 2
                    ) :: bigint / array_length(new.units_ids, 1),
                    0
                )
            where
                new.units_ids && own_unit.units_ids;
        end if;

        return new;
    end if;

end
$body$
language plpgsql;

create trigger cache_fin_totals_for_owner_unit_on_fin_operation
after insert or update of curs, deleted, doc_date, id_currency, is_euro_zone_curs, sum_vat, units_ids or delete
on public.fin_operation
for each row
execute procedure cache_fin_totals_for_owner_unit_on_fin_operation();