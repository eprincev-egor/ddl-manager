import { flatMap } from "lodash";
import { AbstractAstElement } from "./AbstractAstElement";
import { Expression, ConditionElementType } from "./expression";
import { OrderByItem } from "./OrderByItem";
import { Spaces } from "./Spaces";

export class OrderBy extends AbstractAstElement {

    readonly items: readonly OrderByItem[];
    constructor(items: OrderByItem[]) {
        super();
        this.items = items;
    }

    clone() {
        const newItems = this.items.map(item => item.clone());
        return new OrderBy(newItems);
    }

    getColumnReferences() {
        return flatMap(this.items, item => 
            item.getColumnReferences()
        );
    }

    template(spaces: Spaces): string[] {
        return [
            spaces + "order by",
            ...this.items.map((item, i) =>
                item.toSQL( spaces.plusOneLevel() ) + (
                    i === this.items.length - 1 ? "" : ","
                )
            )
        ];
    }

    isOnlyId() {
        const orderByColumns = this.getColumnReferences();
        const byId = (
            orderByColumns.length === 1 &&
            orderByColumns[0].name === "id"
        );
        return byId;
    }

    getFirstColumnRef() {
        return this.getColumnReferences()[0];
    }

    rowIsGreatByOrder(
        greatRow: string,
        lessRow: string,
        orPreConditions: ConditionElementType[] = []
    ) {
        const orderBy = this.items[0]!;
        if ( orderBy.type === "desc" ) {
            return this.rowIsGreat(greatRow, lessRow, orPreConditions);
        }
        else {
            return this.rowIsLess(greatRow, lessRow, orPreConditions);
        }
    }

    rowIsLessByOrder(
        lessRow: string,
        greatRow: string,
        orPreConditions: ConditionElementType[] = []
    ) {
        const orderBy = this.items[0]!;
        if ( orderBy.type === "desc" ) {
            return this.rowIsLess(lessRow, greatRow, orPreConditions);
        }
        else {
            return this.rowIsGreat(lessRow, greatRow, orPreConditions);
        }
    }

    private rowIsGreat(
        greatRow: string,
        lessRow: string,
        orPreConditions: ConditionElementType[] = []
    ) {
        const sortColumnName = this.getFirstColumnRef()!.name;

        return Expression.or([
            ...orPreConditions,
            Expression.and([
                `${greatRow}.${sortColumnName} is not distinct from new.${sortColumnName}`,
                `${greatRow}.id > ${lessRow}.id`
            ]),
            Expression.and([
                `${greatRow}.${sortColumnName} is null`,
                `${lessRow}.${sortColumnName} is not null`
            ]),
            `${greatRow}.${sortColumnName} > ${lessRow}.${sortColumnName}`
        ]);
    }

    private rowIsLess(
        lessRow: string,
        greatRow: string,
        orPreConditions: ConditionElementType[] = []
    ) {
        const sortColumnName = this.getFirstColumnRef()!.name;

        return Expression.or([
            ...orPreConditions,
            Expression.and([
                `${greatRow}.${sortColumnName} is not distinct from new.${sortColumnName}`,
                `${lessRow}.id < ${greatRow}.id`
            ]),
            Expression.and([
                `${lessRow}.${sortColumnName} is not null`,
                `${greatRow}.${sortColumnName} is null`
            ]),
            `${lessRow}.${sortColumnName} < ${greatRow}.${sortColumnName}`
        ]);
    }
}