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
        const sortColumnRef = this.getColumnReferences()[0]!;

        return Expression.or([
            ...orPreConditions,
            Expression.and([
                `${greatRow}.${sortColumnRef.name} is not null`,
                `${lessRow}.${sortColumnRef.name} is null`
            ]),
            `${greatRow}.${sortColumnRef.name} > ${lessRow}.${sortColumnRef.name}`
        ]);
    }

    private rowIsLess(
        lessRow: string,
        greatRow: string,
        orPreConditions: ConditionElementType[] = []
    ) {
        const sortColumnRef = this.getColumnReferences()[0]!;

        return Expression.or([
            ...orPreConditions,
            Expression.and([
                `${lessRow}.${sortColumnRef.name} is null`,
                `${greatRow}.${sortColumnRef.name} is not null`
            ]),
            `${lessRow}.${sortColumnRef.name} < ${greatRow}.${sortColumnRef.name}`
        ]);
    }
}