import { Comment } from "../../lib/database/schema/Comment";
import assert from "assert";

describe("Comment", () => {

    it("correct parsing cache select", () => {
        const selectSQL = `
select
    array_agg(link.id_user) as watchers_users_ids
from user_task_watcher_link as link
where
    link.id_user_task = user_task.id
        `.trim();

        const comment = Comment.fromTotalString("column", `
ddl-manager-sync
ddl-manager-select(${ selectSQL })
ddl-manager-cache(cache watchers for user_task)
        `.trim());

        assert.strictEqual( comment.cacheSelect, selectSQL );
    });

})