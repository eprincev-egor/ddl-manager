import { AbstractAgg, IAggParams } from "./AbstractAgg";
import { ArrayAgg } from "./ArrayAgg";
import { CountAgg } from "./CountAgg";
import { SumAgg } from "./SumAgg";
import { MinAgg } from "./MinAgg";
import { MaxAgg } from "./MaxAgg";

export const aggregatorsMap: {
    [aggName: string]: new (params: IAggParams) => AbstractAgg
} = {
    count: CountAgg,
    sum: SumAgg,
    array_agg: ArrayAgg,
    min: MinAgg,
    max: MaxAgg
};