import {
    Expression,
    HardCode,
    SetItem, SetSelectItem,
    From,
    FuncCall,
    Join,
    Select,
    SelectColumn
} from "../../ast";
import { CacheContext } from "../trigger-builder/CacheContext";
import { TableReference } from "../../database/schema/TableReference";

export class SetItemsFactory {

    protected context: CacheContext;
    constructor(context: CacheContext) {
        this.context = context;
    }

    jsonColumnName() {
        return this.context.cache.jsonColumnName();
    }

    minus() {
        const nextJson = `${this.jsonColumnName()} - old.id::text`;
        return this.setSelect(nextJson);
    }

    plus() {
        const cache = this.context.cache;

        const nextJson = `cm_merge_json(
            ${this.jsonColumnName()},
            null::jsonb,
            ${cache.getSourceRowJson("new")},
            TG_OP
        )`;

        return this.setSelect(nextJson);
    }

    private setSelect(nextJson: string) {
        const cache = this.context.cache;
        const sourceAlias = cache.name;

        return [
            new SetItem({
                column: this.jsonColumnName(),
                value: new HardCode({
                    sql: nextJson
                })
            }),
            new SetSelectItem({

                columns: cache.select.columns
                    .map(column => column.name),

                select: cache.select.clone({
                    from: [new From({
                        source: new Select({
                            columns: [
                                new SelectColumn({
                                    name: "*",
                                    expression: Expression.unknown("record.*")
                                })
                            ],
                            from: [new From({
                                source: new FuncCall(
                                    "jsonb_each",
                                    [Expression.unknown(nextJson)]
                                ),
                                as: "json_entry",

                                joins: [new Join("left join lateral", new HardCode({
                                    sql: `jsonb_populate_record(null::${this.context.triggerTable}, json_entry.value) as record`
                                }), Expression.unknown("true"))]
                            })]
                        }),
                        as: sourceAlias
                    })]
                }).replaceTable(
                    this.context.triggerTable,
                    new TableReference(
                        this.context.triggerTable,
                        sourceAlias
                    )
                ).toString()
            })
        ];
    }
}
