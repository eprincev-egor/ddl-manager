
type CommentObjectType = "function" | "trigger" | "column";

export class Comment {

    static empty(objectType: CommentObjectType) {
        return new Comment({
            objectType,
            frozen: false
        });
    }

    static frozen(objectType: CommentObjectType) {
        return new Comment({
            objectType,
            frozen: true
        });
    }

    static fromTotalString(objectType: CommentObjectType, total: string | null) {
        total = total || "";
        const frozen = !total.includes("ddl-manager-sync");
        const cacheSignature = parseCacheSignature(total);
        const cacheSelect = parseCacheSelect(total);
        const dev = parseDev(total);

        const comment = new Comment({
            objectType,
            frozen,
            dev,
            cacheSignature,
            cacheSelect
        });
        return comment;
    }

    static fromFs(params: {
        objectType: CommentObjectType;
        dev?: string;
        cacheSignature?: string;
        cacheSelect?: string;
    }) {
        const comment = new Comment({
            objectType: params.objectType,
            frozen: false,
            dev: params.dev,
            cacheSignature: params.cacheSignature,
            cacheSelect: params.cacheSelect,
        });
        return comment;
    }

    readonly objectType: CommentObjectType;
    readonly dev?: string;
    readonly frozen?: boolean;
    readonly cacheSignature?: string;
    readonly cacheSelect?: string;

    private constructor(params: {
        objectType: CommentObjectType;
        dev?: string;
        frozen?: boolean;
        cacheSignature?: string;
        cacheSelect?: string;
    }) {
        this.objectType = params.objectType;
        this.dev = params.dev;
        this.frozen = params.frozen;
        this.cacheSignature = params.cacheSignature;
        this.cacheSelect = params.cacheSelect;
        this.validate();
    }

    private validate() {
        if ( this.cacheSignature && this.frozen ) {
            throw new Error("cache object cannot be frozen");
        }

        if ( this.cacheSelect ) {
            if ( this.objectType !== "column" ) {
                throw new Error("cacheSelect can be only for column");
            }

            if ( !this.cacheSignature ) {
                throw new Error("cacheSelect can be only for cache column");
            }
        }
    }

    isEmpty() {
        return this.frozen && !this.dev;
    }

    equal(otherComment: Comment) {
        return this.toString() === otherComment.toString();
    }

    toString() {
        if ( this.frozen && !this.dev ) {
            return "";
        }

        let comment = "";

        if ( this.dev ) {
            comment += this.dev;
        }
        if ( !this.frozen ) {
            comment += "\nddl-manager-sync";
        }
        if ( this.cacheSelect ) {
            comment += `\nddl-manager-select(${ this.cacheSelect })`;
        }
        if ( this.cacheSignature ) {
            comment += `\nddl-manager-cache(${ this.cacheSignature })`;
        }

        return comment;
    }
}

function parseCacheSignature(totalComment: string) {
    const comment = (totalComment || "").trim();
    const matchedResult = comment.match(/ddl-manager-cache\(([^\\)]+)\)/) || [];
    const cacheSignature = matchedResult[1];
    return cacheSignature;
}

function parseCacheSelect(totalComment: string) {
    const comment = (totalComment || "").trim();
    const matchedResult = comment.match(/ddl-manager-select\(([^\\)]+)\)/) || [];
    const cacheSelect = matchedResult[1];
    return cacheSelect;
}

function parseDev(totalComment: string) {
    const devComment = (totalComment || "").trim().split("ddl-manager-sync")[0];
    return devComment.trim();
}