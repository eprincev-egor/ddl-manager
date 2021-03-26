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
        leftRow: string,
        rightRow: string,
        orPreConditions: ConditionElementType[] = []
    ) {
        return this.items[0]!.rowIsGreatByOrder(
            leftRow,
            rightRow,
            orPreConditions
        );
    }

    rowIsLessByOrder(
        leftRow: string,
        rightRow: string,
        orPreConditions: ConditionElementType[] = []
    ) {
        return this.items[0]!.rowIsLessByOrder(
            leftRow,
            rightRow,
            orPreConditions
        );
    }
}