
type CommentObjectType = "function" | "trigger" | "column" | "index";
export interface CommentParams {
    objectType: CommentObjectType;
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

    static frozen(objectType: CommentObjectType) {
        return new Comment({
            objectType,
            frozen: true
        });
    }

    static fromTotalString(objectType: CommentObjectType, total: string | null) {
        total = total || "";
        const frozen = (
            !total.includes("ddl-manager-sync") &&
            !total.includes("ddl-manager-helper")
        );
        const cacheSignature = parseCacheSignature(total);
        const cacheSelect = parseCacheSelect(total);

        const comment = new Comment({
            objectType,
            frozen,
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
    readonly frozen: boolean;
    readonly cacheSignature?: string;
    readonly cacheSelect?: string;
    readonly legacy?: boolean;

    private constructor(params: CommentParams) {
        this.objectType = params.objectType;
        this.frozen = !!params.frozen;
        this.cacheSignature = params.cacheSignature;
        this.cacheSelect = params.cacheSelect;
    }

    isEmpty() {
        return this.frozen && !this.cacheSelect;
    }

    equal(otherComment: Comment) {
        return (
            this.cacheSelect == otherComment.cacheSelect &&
            this.cacheSignature == otherComment.cacheSignature
        );
    }

    markAsFrozen() {
        return new Comment({
            ...this,
            frozen: true
        });
    }

    toString() {
        let comment = "";

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
