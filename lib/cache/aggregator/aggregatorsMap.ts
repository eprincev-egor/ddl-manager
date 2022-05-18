import { AbstractAgg, IAggParams } from "./AbstractAgg";
import { ArrayAgg } from "./ArrayAgg";
import { ArrayUnionAgg } from "./ArrayUnionAgg";
import { ArrayUnionAllAgg } from "./ArrayUnionAllAgg";
import { CountAgg } from "./CountAgg";
import { SumAgg } from "./SumAgg";

export const aggregatorsMap: {
    [aggName: string]: new (params: IAggParams) => AbstractAgg
} = {
    count: CountAgg,
    sum: SumAgg,
    array_agg: ArrayAgg,
    array_union_agg: ArrayUnionAgg,
    array_union_all_agg: ArrayUnionAllAgg
};