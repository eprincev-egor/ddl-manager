import { flatMap } from "lodash";
import { AbstractAstElement } from "./AbstractAstElement";
import { ConditionElementType } from "./expression";
import { OrderByItem, CompareRow } from "./OrderByItem";
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

    compareRowsByOrder(
        leftRow: CompareRow,
        operator: "<" | ">",
        rightRow: CompareRow,
        orPreConditions: ConditionElementType[] = []
    ) {
        return this.items[0]!.compareRowsByOrder(
            leftRow,
            operator,
            rightRow,
            orPreConditions
        );
    }
}