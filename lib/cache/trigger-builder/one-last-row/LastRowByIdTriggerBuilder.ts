import {
    Update, Expression, SetItem,
    SelectColumn, UnknownExpressionElement, 
    SimpleSelect,
    Spaces
} from "../../../ast";
import { Exists } from "../../../ast/expression/Exists";
import { Comment } from "../../../database/schema/Comment";
import { DatabaseFunction } from "../../../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../../database/schema/DatabaseTrigger";
import { ICacheTrigger } from "../AbstractTriggerBuilder";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByIdBody } from "../body/buildOneLastRowByIdBody";

export class LastRowByIdTriggerBuilder extends AbstractLastRowTriggerBuilder {

    createHelperTrigger(): ICacheTrigger | undefined {
        const orderBy = this.context.cache.select.orderBy[0]!;
        if ( orderBy.type === "asc" ) {
            return;
        }

        const isLastColumnName = this.getIsLastColumnName();

        const helperTriggerName = [
            "cache",
            this.context.cache.name,
            "before",
            "insert",
            this.context.triggerTable.name
        ].join("_");

        const trigger = new DatabaseTrigger({
            name: helperTriggerName,
            before: true,
            insert: true,
            procedure: {
                schema: "public",
                name: helperTriggerName,
                args: []
            },
            table: this.context.triggerTable,
            comment: Comment.fromFs({
                objectType: "trigger",
                cacheSignature: this.context.cache.getSignature()
            })
        });

        return {
            trigger,
            function: new DatabaseFunction({
                schema: "public",
                name: helperTriggerName,
                body: `
    begin

    new.${isLastColumnName} = (
        ${this.conditions
            .hasReferenceWithoutJoins("new")!
            .toSQL( Spaces.level(2) )}
    );

    return new;
    end;
                `.trim(),
                comment: Comment.fromFs({
                    objectType: "function",
                    cacheSignature: this.context.cache.getSignature()
                }),
                args: [],
                returns: {type: "trigger"}
            })
        };
    }

    protected createBody() {

        const updateNew = new Update({
            table: this.context.cache.for.toString(),
            set: this.setItemsByRow("new"),
            where: Expression.and([
                this.conditions.simpleWhere("new")!,
                this.whereDistinctRowValues("new")
            ])
        });
        const updatePrev = new Update({
            table: this.context.cache.for.toString(),
            set: this.setItemsByRow("prev_row"),
            where: Expression.and([
                this.conditions.simpleWhere("old")!,
                this.whereDistinctRowValues("prev_row")
            ])
        });

        const isLastColumnName = this.getIsLastColumnName();
        const triggerTable = this.context.triggerTable.toStringWithoutPublic();

        const updatePrevRowLastColumnTrue = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: UnknownExpressionElement.fromSql("true")
            })],
            where: Expression.and([
                `${triggerTable}.id = prev_row.id`
            ])
        });

        const updateMaxRowLastColumnFalse = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: UnknownExpressionElement.fromSql("false")
            })],
            where: Expression.and([
                `${triggerTable}.id = prev_id`,
                `${isLastColumnName} = true`
            ])
        });

        const updateThisRowLastColumnTrue = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: UnknownExpressionElement.fromSql("true")
            })],
            where: Expression.and([
                `${triggerTable}.id = new.id`
            ])
        });

        const clearLastColumnOnInsert = new Update({
            table: triggerTable,
            set: [new SetItem({
                column: isLastColumnName,
                value: UnknownExpressionElement.fromSql("false")
            })],
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = new.${column}`
                ),
                `${triggerTable}.id < new.id`,
                `${isLastColumnName} = true`
            ])
        });

        const selectPrevRowColumnsNames = this.context.triggerTableColumns.slice();
        if  ( !selectPrevRowColumnsNames.includes("id") ) {
            selectPrevRowColumnsNames.unshift("id");
        }

        const selectPrevRowColumns: SelectColumn[] = selectPrevRowColumnsNames.map(name =>
            new SelectColumn({
                name,
                expression: Expression.unknown(name)
            })
        );

        const orderBy = this.context.cache.select.orderBy[0]!;
        const selectMaxPrevId = new SimpleSelect({
            columns: [
                `${ orderBy.type == "desc" ? "max" : "min" }( ${ triggerTable }.id )`
            ],
            from: this.context.triggerTable,
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = new.${column}`
                ),
                ...this.context.referenceMeta.filters,
                `${triggerTable}.id <> new.id`
            ])
        });

        const selectPrevRow = this.context.cache.select.cloneWith({
            columns: selectPrevRowColumns,
            where: Expression.and([
                ...this.context.referenceMeta.columns.map(column =>
                    `${triggerTable}.${column} = old.${column}`
                ),
                ...this.context.referenceMeta.filters
            ]),
            intoRow: "prev_row"
        });
        const existsPrevRow = new Exists({
            select: this.context.cache.select.cloneWith({
                columns: [],
                where: Expression.and([
                    ...this.context.referenceMeta.columns.map(column =>
                        `${triggerTable}.${column} = new.${column}`
                    ),
                    ...this.context.referenceMeta.filters,
                    `${triggerTable}.id < new.id`
                ]),
                orderBy: [],
                limit: undefined
            })
        });

        const body = buildOneLastRowByIdBody({
            orderVector: orderBy.type,

            isLastColumn: isLastColumnName,
            ifNeedUpdateNewOnChangeReference: Expression.or([
                `prev_id ${orderBy.type === "desc" ? "<" : ">"} new.id`,
                "prev_id is null"
            ]),
            updateNew,

            exitFromDeltaUpdateIf: this.conditions.exitFromDeltaUpdateIf(),

            selectMaxPrevId,
            selectPrevRow,
            existsPrevRow,
            updatePrev,

            updateMaxRowLastColumnFalse,
            updateThisRowLastColumnTrue,

            clearLastColumnOnInsert,
            updatePrevRowLastColumnTrue,

            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,
            hasOldReferenceAndIsLast: Expression.and([
                this.conditions.hasReferenceWithoutJoins("old")!,
                UnknownExpressionElement.fromSql(`old.${isLastColumnName}`)
            ]),
            noChanges: this.conditions.noChanges(),
            noReferenceChanges: this.conditions.noReferenceChanges()
        });
        return body;
    }

}