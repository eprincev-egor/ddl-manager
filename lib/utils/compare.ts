
interface IModel {
    equal(other: this): boolean;
}

interface IMap<TModel> {
    [id: string]: TModel;
}

interface IChanges<TModel> {
    created: TModel[];
    removed: TModel[];
    changed: {prev: TModel, next: TModel}[];
}

export function compareMapWithDestination<T extends IModel>(
    sourceMap: IMap<T>,
    destinationMap: IMap<T>
): IChanges<T> {
    const changes: IChanges<T> = {
        created: [],
        changed: [],
        removed: []
    };

    for (const identify in sourceMap) {
        const sourceObject = sourceMap[ identify ];
        const destinationObject = destinationMap[ identify ];

        if ( destinationObject ) {
            if ( !sourceObject.equal(destinationObject) ) {
                changes.changed.push({
                    prev: sourceObject,
                    next: destinationObject
                });
            }
        }
        else {
            changes.removed.push(sourceObject);
        }
    }
    for (const identify in destinationMap) {
        if ( identify in sourceMap ) {
            continue;
        }
        const destinationObject = destinationMap[ identify ];
        changes.created.push(destinationObject);
    }

    return changes;
}