import _ from "lodash";
import { IDiff } from "./interface";

export function logDiff(diff: IDiff) {
    diff.drop.triggers.forEach((trigger: any) => {
        const triggerIdentifySql = trigger.getSignature();
        // tslint:disable-next-line: no-console
        console.log("drop trigger " + triggerIdentifySql);
    });

    diff.drop.functions.forEach((func: any) => {
        const funcIdentifySql = func.getSignature();
        // tslint:disable-next-line: no-console
        console.log("drop function " + funcIdentifySql);
    });
    
    diff.create.functions.forEach((func: any) => {
        const funcIdentifySql = func.getSignature();
        // tslint:disable-next-line: no-console
        console.log("create function " + funcIdentifySql);
    });

    diff.create.triggers.forEach((trigger: any) => {
        const triggerIdentifySql = trigger.getSignature();
        // tslint:disable-next-line: no-console
        console.log("create trigger " + triggerIdentifySql);
    });
}
