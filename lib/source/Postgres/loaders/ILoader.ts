import { IDBO } from "../../../common";

export interface ILoader {
    load(): Promise<IDBO[]>;
}