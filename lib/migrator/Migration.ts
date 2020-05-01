import { MigrationCommand } from "./MigrationCommand";
import { MigrationError } from "./MigrationError";
import { IDBO } from "../common";

export class Migration {
    commands: MigrationCommand[] = [];
    errors: MigrationError[] = [];

    addError(err: MigrationError) {
        this.errors.push(err);
    }

    addCommand(type: MigrationCommand["type"], object: IDBO) {
        const command = new MigrationCommand({
            type,
            object
        });
        this.commands.push(command);
    }
}