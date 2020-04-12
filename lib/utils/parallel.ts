
export async function parallel(promises: Promise<any>[]) {
    await Promise.all(promises);
}