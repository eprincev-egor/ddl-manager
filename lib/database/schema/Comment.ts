
type CommentObjectType = "function" | "trigger" | "column" | "index";
export interface CommentParams {
    objectType: CommentObjectType;
    dev?: string;
    cacheSignature?: string;
    cacheSelect?: string;
    frozen?: boolean;
}

export class Comment {

    static empty(objectType: CommentObjectType) {
        return new Comment({
            objectType,
            frozen: false
        });
    }

    static frozen(objectType: CommentObjectType, dev?: string) {
        return new Comment({
            objectType,
            frozen: true,
            dev
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
            cacheSelect,
        });
        return comment;
    }

    static fromFs(params: CommentParams) {
        return new Comment({
            ...params,
            frozen: false
        });
    }

    readonly objectType: CommentObjectType;
    readonly dev?: string;
    readonly frozen?: boolean;
    readonly cacheSignature?: string;
    readonly cacheSelect?: string;
    readonly legacy?: boolean;

    private constructor(params: CommentParams) {
        this.objectType = params.objectType;
        this.dev = params.dev;
        this.frozen = params.frozen;
        this.cacheSignature = params.cacheSignature;
        this.cacheSelect = params.cacheSelect;
        this.validate();
    }

    private validate() {
        if ( !this.cacheSelect ) {
            return;
        }

        if ( this.objectType !== "column" ) {
            throw new Error("cacheSelect can be only for column");
        }

        if ( !this.cacheSignature ) {
            throw new Error("cacheSelect can be only for cache column");
        }
    }

    isEmpty() {
        return this.frozen && !this.dev;
    }

    equal(otherComment: Comment) {
        return this.toString() === otherComment.toString();
    }

    markAsFrozen() {
        return new Comment({
            ...this,
            frozen: true
        });
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

    const codePhrase = "ddl-manager-select";
    const startParsing = comment.indexOf(codePhrase + "(");
    if ( startParsing === -1 ) {
        return undefined;
    }

    let cacheSelect = "";
    let openedBracketsCount = 1;
    for (let i = startParsing + codePhrase.length + 1; i < comment.length; i++) {
        const symbol = comment[i];

        if ( symbol === "(" ) {
            openedBracketsCount++;
        }
        if ( symbol === ")" ) {
            openedBracketsCount--;
            if ( openedBracketsCount === 0 ) {
                break;
            }
        }

        cacheSelect += symbol;
    }

    return cacheSelect;
}

function parseDev(totalComment: string) {
    const devComment = (totalComment || "").trim().split("ddl-manager-sync")[0];
    return devComment.trim();
}