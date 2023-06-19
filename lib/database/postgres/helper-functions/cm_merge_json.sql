create or replace function cm_merge_json(
    current_json jsonb,
    old_item jsonb,
    new_item jsonb,
    tg_op text
)
returns jsonb as $body$
declare row_id text;
begin
    current_json = coalesce(current_json, '{}'::jsonb);
    row_id = new_item->'id';

    -- when exists custom trigger 
    -- who made update current row on insert
    -- then TG_OP INSERT can be handled after TG_OP UPDATE
    --
    if tg_op = 'INSERT' then
        if current_json->row_id is not null then
            return current_json;
        end if;
    end if;


    return jsonb_set(
        current_json,
        ARRAY[ row_id ]::text[],
        new_item
    );
end
$body$
language plpgsql
immutable;