
export function findMinimalRoute(params: {
    graph: string[][], 
    start: string, 
    end: string
}): string[] | undefined {

    const allRoutes: string[][] = [];
    findAllRoutes({
        graph: params.graph, 
        start: params.start, 
        end: params.end, 
        allRoutes, 
        prevRoute: []
    });
    
    let minimalRoute: string[] = allRoutes[0];
    for (let i = 1, n = allRoutes.length; i < n; i++) {
        const route = allRoutes[i];
        if ( route.length < minimalRoute.length ) {
            minimalRoute = route;
        }
    }

    return minimalRoute;
}

function findAllRoutes(params: {
    graph: string[][], 
    start: string, 
    end: string,
    allRoutes: string[][]
    prevRoute: string[]
}) {

    const currentRoute = [...params.prevRoute, params.start];
    if ( params.start === params.end ) {
        params.allRoutes.push(currentRoute);
        return;
    }

    for (const points of params.graph) {
        const lineHasStart = points.includes(params.start);

        if ( !lineHasStart ) {
            continue;
        }

        const nextPoints = points.filter(point =>
            point !== params.start
        );

        for (const nextPoint of nextPoints) {
            const pointExistsInCurrentRoute = currentRoute.includes(nextPoint);
            if ( pointExistsInCurrentRoute ) {
                continue;
            }

            findAllRoutes({
                graph: params.graph,
                start: nextPoint,
                end: params.end,
                allRoutes: params.allRoutes,
                prevRoute: currentRoute
            });
        }
    }
}
