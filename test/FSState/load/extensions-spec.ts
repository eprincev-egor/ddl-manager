import {TestState, ITestFiles} from "../TestState";

describe("FSDDLState", () => {

    it("load dir with one file with extension", async() => {
        const files: ITestFiles = {
            "test.sql": [
                {
                    type: "extension",
                    sql: `
                        extension test 
                        for companies (
                            inn text
                        )
                    `,
                    row: {
                        identify: "extension test for public.companies",
                        forTableIdentify: "public.companies",
                        name: "test",
                        columns: [{
                            filePath: "test.sql",
                            identify: "inn",
                            key: "inn",
                            type: "text"
                        }]
                    }
                }
            ]
        };

        await TestState.testLoading({
            files,
            expectedState: {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "test.sql",
                            name: "test.sql",
                            content: TestState.concatFilesSql( files["test.sql"] )
                        }
                    ],
                    folders: []
                },
                functions: [],
                triggers: [],
                tables: [],
                views: [],
                extensions: [
                    {
                        filePath: "test.sql",
                        identify: "extension test for public.companies",
                        forTableIdentify: "public.companies",
                        name: "test",
                        parsed: null,
                        columns: [
                            {
                                filePath: "test.sql",
                                identify: "inn",
                                key: "inn",
                                type: "text",
                                nulls: true,
                                parsed: null
                            }
                        ],
                        deprecated: false,
                        deprecatedColumns: [],
                        primaryKey: null,
                        checkConstraints: [],
                        foreignKeysConstraints: [],
                        uniqueConstraints: [],
                        values: null
                    }
                ]
            }
        });

    });
    
    it("load dir with one file with two extensions", async() => {
        const files: ITestFiles = {
            "test.sql": [
                {
                    type: "extension",
                    sql: `
                        extension test1
                        for companies (
                            inn text
                        )
                    `,
                    row: {
                        identify: "extension test1 for public.companies",
                        forTableIdentify: "public.companies",
                        name: "test1",
                        columns: [{
                            filePath: "test.sql",
                            identify: "inn",
                            key: "inn",
                            type: "text"
                        }]
                    }
                },
                {
                    type: "extension",
                    sql: `
                        extension test2
                        for companies (
                            note text
                        )
                    `,
                    row: {
                        identify: "extension test2 for public.companies",
                        forTableIdentify: "public.companies",
                        name: "test2",
                        columns: [{
                            filePath: "test.sql",
                            identify: "note",
                            key: "note",
                            type: "text"
                        }]
                    }
                }
            ]
        };

        await TestState.testLoading({
            files,
            expectedState: {
                folder: {
                    path: "./",
                    name: "",
                    files: [
                        {
                            path: "test.sql",
                            name: "test.sql",
                            content: TestState.concatFilesSql( files["test.sql"] )
                        }
                    ],
                    folders: []
                },
                functions: [],
                triggers: [],
                tables: [],
                views: [],
                extensions: [
                    {
                        filePath: "test.sql",
                        identify: "extension test1 for public.companies",
                        forTableIdentify: "public.companies",
                        name: "test1",
                        parsed: null,
                        columns: [
                            {
                                filePath: "test.sql",
                                identify: "inn",
                                key: "inn",
                                type: "text",
                                nulls: true,
                                parsed: null
                            }
                        ],
                        deprecated: false,
                        deprecatedColumns: [],
                        primaryKey: null,
                        checkConstraints: [],
                        foreignKeysConstraints: [],
                        uniqueConstraints: [],
                        values: null
                    },
                    {
                        filePath: "test.sql",
                        identify: "extension test2 for public.companies",
                        forTableIdentify: "public.companies",
                        name: "test2",
                        parsed: null,
                        columns: [
                            {
                                filePath: "test.sql",
                                identify: "note",
                                key: "note",
                                type: "text",
                                nulls: true,
                                parsed: null
                            }
                        ],
                        deprecated: false,
                        deprecatedColumns: [],
                        primaryKey: null,
                        checkConstraints: [],
                        foreignKeysConstraints: [],
                        uniqueConstraints: [],
                        values: null
                    }
                ]
            }
        });

    });
    
});