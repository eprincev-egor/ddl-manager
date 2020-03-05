import TestFSDriver from "./TestFSDriver";
import TestParser from "./TestParser";
import FunctionModel from "../../lib/objects/FunctionModel";
import BaseDBObjectModel from "../../lib/objects/BaseDBObjectModel";
import TableModel from "../../lib/objects/TableModel";
import ViewModel from "../../lib/objects/ViewModel";
import TriggerModel from "../../lib/objects/TriggerModel";
import FSState from "../../lib/FSState";
import assert from "assert";
import {sleep} from "../utils";

export type TTestModel = {
    type: "function";
    sql: string;
    row: FunctionModel["TInputData"];
} | {
    type: "table";
    sql: string;
    row: TableModel["TInputData"]
} | {
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

export class TestState {
    driver: TestFSDriver;
    parser: TestParser;
    fsState: FSState;

    static concatFilesSql(models: TTestModel[]) {
        return models.map(model => model.sql).join("\n");
    }

    static async testLoading(test: ITest) {
        const {fsState} = new TestState(test.files);
        await fsState.load("./");

        const actualState = fsState.toJSON();
        assert.deepStrictEqual(actualState, test.expectedState);
    }

    constructor(files: ITestFiles) {
        this.driver = new TestFSDriver({});
        this.parser = new TestParser({});
        
        for (const filePath in files) {
            const objects = files[ filePath ];

            this.addTestFile(filePath, objects);
        }
    
        this.fsState = new FSState({
            driver: this.driver,
            parser: this.parser
        });
    }

    addTestFile(filePath: string, testModels: TTestModel[]) {

        const models: BaseDBObjectModel<any>[] = [];
        const sql = TestState.concatFilesSql(testModels);

        for (const testModel of testModels) {

            if ( testModel.type === "function" ) {
                models.push(
                    new FunctionModel({
                        ...testModel.row,
                        filePath
                    })
                );
            }

            if ( testModel.type === "table" ) {
                models.push(
                    new TableModel({
                        ...testModel.row,
                        filePath
                    })
                );
            }

            if ( testModel.type === "view" ) {
                models.push(
                    new ViewModel({
                        ...testModel.row,
                        filePath
                    })
                );
            }

            if ( testModel.type === "trigger" ) {
                models.push(
                    new TriggerModel({
                        ...testModel.row,
                        filePath
                    })
                );
            }
        }

        this.driver.addTestFile(filePath, sql);
        this.parser.addTestFile(sql, models);
    }

    getFileSQL(filePath: string) {
        return this.driver.files[ filePath ];
    }

    async emitFS(eventType: string, path: string) {
        this.driver.emit(eventType, path);
        await sleep(1000);
    }
}
