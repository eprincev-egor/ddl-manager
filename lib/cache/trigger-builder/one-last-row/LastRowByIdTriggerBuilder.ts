import {
    Update, SetItem,
    Expression,
    SimpleSelect,
    Spaces
} from "../../../ast";
import { Exists } from "../../../ast/expression/Exists";
import { Comment } from "../../../database/schema/Comment";
import { DatabaseFunction } from "../../../database/schema/DatabaseFunction";
import { DatabaseTrigger } from "../../../database/schema/DatabaseTrigger";
import { AbstractLastRowTriggerBuilder } from "./AbstractLastRowTriggerBuilder";
import { buildOneLastRowByIdBody } from "../body/buildOneLastRowByIdBody";

export class LastRowByIdTriggerBuilder 
extends AbstractLastRowTriggerBuilder {

    createTriggers() {
        return [{
            trigger: this.createDatabaseTriggerOnDIU(),
            procedure: this.createDatabaseFunction(
                this.createBody()
            )
        }, ...this.createHelperTriggers()];
    }

    protected createHelperTriggers() {
        const orderBy = this.context.cache.select.orderBy!.items[0]!;
        if ( orderBy.type === "asc" ) {
            return [];
        }

        const isLastColumnName = this.getIsLastColumnName();

        const helperTriggerName = [
            "cache",
            this.context.cache.name,
            "for",
            this.context.cache.for.table.name,
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

        return [{
            trigger,
            procedure: new DatabaseFunction({
                schema: "public",
                name: helperTriggerName,
                body: `
    begin

    new.${isLastColumnName} = coalesce((
        ${this.conditions
            .hasReferenceWithoutJoins("new")!
            .toSQL( Spaces.level(2) )}
    ), false);

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
        }];
    }

    protected createBody() {

        const isLastColumnName = this.getIsLastColumnName();
        const triggerTable = this.triggerTableAlias();

        const clearLastColumnOnInsert = new Update({
            table: this.fromTable().toString(),
            set: [new SetItem({
                column: isLastColumnName,
                value: Expression.unknown("false")
            })],
            where: this.filterTriggerTable("new", [
                `${triggerTable}.id < new.id`,
                `${triggerTable}.${isLastColumnName} = true`
            ])
        });

        const orderBy = this.context.cache.select.orderBy!.items[0]!;
        const selectMaxPrevId = new SimpleSelect({
            columns: [
                `${ orderBy.type == "desc" ? "max" : "min" }( ${ triggerTable }.id )`
            ],
            from: this.context.cache.select.getFromTable(),
            where: this.filterTriggerTable("new", [
                `${triggerTable}.id <> new.id`
            ])
        });

        const existsPrevRow = new Exists({
            select: this.context.cache.select.clone({
                columns: [],
                where: this.filterTriggerTable("new", [
                    `${triggerTable}.id < new.id`
                ]),
                orderBy: undefined,
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

            updateNew: this.updateNew(),
            updatePrev: this.updatePrev(),

            exitFromDeltaUpdateIf: this.conditions.exitFromDeltaUpdateIf(),

            selectMaxPrevId,
            selectPrevRow: this.selectPrevRowByOrder(),
            existsPrevRow,

            updateMaxRowLastColumnFalse: this.updateMaxRowLastColumnFalse("prev_id"),
            updateThisRowLastColumnTrue: this.updateThisRowLastColumn("true"),

            clearLastColumnOnInsert,
            updatePrevRowLastColumnTrue: this.updatePrevRowLastColumnTrue(),

            hasNewReference: this.conditions
                .hasReferenceWithoutJoins("new")!,
            hasOldReference: this.conditions
                .hasReferenceWithoutJoins("old")!,
            hasOldReferenceAndIsLast: Expression.and([
                this.conditions.hasReferenceWithoutJoins("old")!,
                `old.${isLastColumnName}`
            ]),
            noChanges: this.conditions.noChanges(),
            noReferenceChanges: this.conditions.noReferenceChanges()
        });
        return body;
    }

}