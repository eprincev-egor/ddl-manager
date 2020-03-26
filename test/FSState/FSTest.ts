import {TestFSDriver} from "./FaketFSDriver";
import {TestParser} from "./FakeParser";
import {FunctionModel} from "../../lib/objects/FunctionModel";
import {BaseDBObjectModel} from "../../lib/objects/base-layers/BaseDBObjectModel";
import {TableModel} from "../../lib/objects/TableModel";
import {ViewModel} from "../../lib/objects/ViewModel";
import {TriggerModel} from "../../lib/objects/TriggerModel";
import {ExtensionModel} from "../../lib/objects/ExtensionModel";
import {FSDDLState} from "../../lib/state/FSDDLState";
import {FSController} from "../../lib/fs/FSController";
import assert from "assert";
import {sleep} from "../utils";

export type TTestModel = {
    type: "function";
    sql: string;
    row: FunctionModel["TInputData"];
} | {
    type: "table";
    sql: string;
    row: TableModel["TInputData"];
} | {
    type: "view";
    sql: string;
    row: ViewModel["TInputData"];
} | {
    type: "trigger";
    sql: string;
    row: TriggerModel["TInputData"];
} | {
    type: "extension";
    sql: string;
    row: ExtensionModel["TInputData"];
};

export interface ITestFiles {
    [filePath: string]: TTestModel[]
}

interface ITest {
    files: ITestFiles;
    expectedState: FSDDLState["TJson"];
}

export class FSTest {
    driver: TestFSDriver;
    parser: TestParser;
    fsState: FSDDLState;
    controller: FSController;

    static concatFilesSql(models: TTestModel[]) {
        return models.map(model => model.sql).join("\n");
    }

    static async testLoading(test: ITest) {
        const {fsState, controller} = new FSTest(test.files);
        await controller.load("./");

        const actualState = fsState.toJSON();
        assert.deepStrictEqual(actualState, test.expectedState);
    }

    constructor(files: ITestFiles) {
        this.driver = new TestFSDriver({});
        this.parser = new TestParser({});
        
        for (const filePath in files) {
            const objects = files[ filePath ];

            this.setTestFile(filePath, objects);
        }
    
        this.fsState = new FSDDLState();
        this.controller = new FSController({
            driver: this.driver,
            parser: this.parser,
            state: this.fsState
        });
    }

    async testLoading(expectedState: FSDDLState["TJson"]) {
        await this.controller.load("./");

        assert.deepEqual(
            this.fsState.toJSON(),
            expectedState
        );
    }

    setTestFile(filePath: string, testModels: TTestModel[]) {

        const models: BaseDBObjectModel<any>[] = [];
        const sql = FSTest.concatFilesSql(testModels);

        for (const testModel of testModels) {
            const dboModel = this.createTestDBOModel(filePath, testModel);
            models.push( dboModel );
        }

        this.driver.setTestFile(filePath, sql);
        this.parser.setTestFile(filePath, sql, models);
    }

    removeTestFile(filePath: string) {
        this.driver.removeTestFile(filePath);
        this.parser.removeTestFile(filePath);
    }

    moveTestFile(oldFilePath: string, newFilePath: string) {
        const sql = this.getFileSQL(oldFilePath);
        const models = this.parser.parseFile(oldFilePath, sql);

        this.removeTestFile(oldFilePath);

        models.forEach(model => {
            model.set({
                filePath: newFilePath
            });
        });

        this.driver.setTestFile(newFilePath, sql);
        this.parser.setTestFile(newFilePath, sql, models);
    }

    createTestDBOModel(filePath: string, testModel: TTestModel): BaseDBObjectModel<any> {
        let outputDBOModel: BaseDBObjectModel<any>;

        if ( testModel.type === "function" ) {
            outputDBOModel = new FunctionModel({
                ...testModel.row,
                filePath
            });
        }

        if ( testModel.type === "table" ) {
            outputDBOModel = new TableModel({
                ...testModel.row,
                filePath
            });
        }

        if ( testModel.type === "view" ) {
            outputDBOModel = new ViewModel({
                ...testModel.row,
                filePath
            });
        }

        if ( testModel.type === "trigger" ) {
            outputDBOModel = new TriggerModel({
                ...testModel.row,
                filePath
            });
        }

        if ( testModel.type === "extension" ) {
            outputDBOModel = new ExtensionModel({
                ...testModel.row,
                filePath
            });
        }

        return outputDBOModel;
    }

    getFileSQL(filePath: string) {
        return this.driver.getFile( filePath );
    }

    getFileJSON(filePath: string) {
        const fileName = filePath.split("/").pop();
        const fileContent = this.driver.getFile( filePath );
        const dbObjects = this.parser.parseFile( filePath, fileContent ).map(dbo =>
            dbo.toJSON()
        );

        return {
            path: filePath,
            name: fileName,
            content: fileContent,
            objects: dbObjects
        };
    }

    async emitFS(eventType: string, path: string) {
        this.driver.emit(eventType, path);
        await sleep(10);
    }
}
