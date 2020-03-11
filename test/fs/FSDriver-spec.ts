import FSDriver from "../../lib/fs/FSDriver";
import assert from "assert";

describe("FSDriver", () => {

    it("read file", async() => {
        const filePath = __dirname + "/fs-driver/test.sql";
        const fsDriver = new FSDriver();
        const fileContent = await fsDriver.readFile(filePath);

        assert.strictEqual(fileContent, "test");
    });

    it("read dir", async() => {
        const folderPath = __dirname + "/fs-driver/test";
        const fsDriver = new FSDriver();
        const {files, directories} = await fsDriver.readFolder(folderPath);

        assert.deepStrictEqual(files, ["x.sql"]);
        assert.deepStrictEqual(directories, ["a"]);
    });

    it("read nonexistent file", async() => {
        const filePath = __dirname + "/fs-driver/unknown";
        const fsDriver = new FSDriver();
        
        let hasError = false;
        try {
            await fsDriver.readFile(filePath);
        } catch(err) {
            hasError = true;
            assert.ok(/no such file or directory/.test(err.message));
        }

        assert.ok(hasError, "expected error");
    });


    it("read nonexistent folder", async() => {
        const folderPath = __dirname + "/fs-driver/unknown";
        const fsDriver = new FSDriver();
        
        let hasError = false;
        try {
            await fsDriver.readFolder(folderPath);
        } catch(err) {
            hasError = true;
            assert.ok(/no such file or directory/.test(err.message));
        }

        assert.ok(hasError, "expected error");
    });

});