
const ONE_LEVEL_SPACES = "    ";

export class Spaces {
    static empty() {
        return new Spaces();
    }
    
    private constructor(private readonly level: number = 0) {}

    toString() {
        let outputSpaces = "";

        for (let i = 0; i < this.level; i++) {
            outputSpaces += ONE_LEVEL_SPACES;
        }

        return outputSpaces;
    }

    plusOneLevel(): Spaces {
        return new Spaces(this.level + 1);
    }
}