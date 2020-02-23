import FSState from "../../../lib/FSState";
import TestFSDriver, {IFiles} from "../TestFSDriver";
import TestParser, {IState} from "../TestParser";
import FunctionModel from "../../../lib/objects/FunctionModel";
import BaseDBObjectModel from "../../../lib/objects/BaseDBObjectModel";
import TableModel from "../../../lib/objects/TableModel";
import assert from "assert";

type TTestModel = {
    type: "function";
    row: FunctionModel["TInputData"];
} | {
    type: "table";
    row: TableModel["TInputData"]
};

export interface ITestFiles {
    [filePath: string]: {
        sql: string;
        models: TTestModel[]
    }
}

interface ITest {
    files: ITestFiles;
    expectedState: FSState["TJson"];
}

export default async function testLoadState(test: ITest) {

    const driverParams: IFiles = {};
    const parserParams: IState = {};

    for (const filePath in test.files) {
        const {sql, models} = test.files[ filePath ];
        driverParams[ filePath ] = sql;

        const objects: BaseDBObjectModel<any>[] = [];
        for (const model of models) {

            if ( model.type === "function" ) {
                objects.push(
                    new FunctionModel({
                        ...model.row,
                        filePath
                    })
                );
            }

            if ( model.type === "table" ) {
                objects.push(
                    new TableModel({
                        ...model.row,
                        filePath
                    })
                );
            }

        }

        parserParams[ sql ] = objects;
    }

    const fsDriver = new TestFSDriver(driverParams);
    const testParser = new TestParser(parserParams);
    
    const fsState = new FSState({
        driver: fsDriver,
        parser: testParser
    });

    await fsState.load("./");

    assert.deepStrictEqual(fsState.toJSON(), test.expectedState);
}