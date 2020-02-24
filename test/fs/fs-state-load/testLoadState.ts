import FSState from "../../../lib/FSState";
import TestFSDriver, {IFiles} from "../TestFSDriver";
import TestParser, {IState} from "../TestParser";
import FunctionModel from "../../../lib/objects/FunctionModel";
import BaseDBObjectModel from "../../../lib/objects/BaseDBObjectModel";
import TableModel from "../../../lib/objects/TableModel";
import assert from "assert";
import ViewModel from "../../../lib/objects/ViewModel";
import TriggerModel from "../../../lib/objects/TriggerModel";

type TTestModel = {
    type: "function";
    sql: string;
    row: FunctionModel["TInputData"];
} | {
    type: "table";
    sql: string;
    row: TableModel["TInputData"]
}| {
    type: "view";
    sql: string;
    row: ViewModel["TInputData"]
} | {
    type: "trigger";
    sql: string;
    row: TriggerModel["TInputData"]
};

export interface ITestFiles {
    [filePath: string]: TTestModel[]
}

interface ITest {
    files: ITestFiles;
    expectedState: FSState["TJson"];
}

export default async function testLoadState(test: ITest) {

    const driverParams: IFiles = {};
    const parserParams: IState = {};

    for (const filePath in test.files) {
        const models = test.files[ filePath ];
        const sql = testLoadState.getFileSql(models);

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

            if ( model.type === "view" ) {
                objects.push(
                    new ViewModel({
                        ...model.row,
                        filePath
                    })
                );
            }

            if ( model.type === "trigger" ) {
                objects.push(
                    new TriggerModel({
                        ...model.row,
                        filePath
                    })
                );
            }
        }

        driverParams[ filePath ] = sql;
        parserParams[ sql ] = objects;
    }

    const fsDriver = new TestFSDriver(driverParams);
    const testParser = new TestParser(parserParams);
    
    const fsState = new FSState({
        driver: fsDriver,
        parser: testParser
    });

    await fsState.load("./");

    const actualState = fsState.toJSON();
    assert.deepStrictEqual(actualState, test.expectedState);
}

testLoadState.getFileSql = (models: TTestModel[]) => {
    return models.map(model => model.sql).join("\n");
};