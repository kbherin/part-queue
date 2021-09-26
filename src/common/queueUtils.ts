import { Redis } from "ioredis";

/**
 * Expire visibility of jobs in 'processing' set based on previously supplied timeout.
 * @param redis
 * @param invisibleSet 
 * @param workQueue 
 * @returns 
 */
export function expireVisibilityFn(redis: Redis, invisibleSet:string, workQueue:string) {
    return () => {
        return redis.eval(`
            local currentTime = ARGV[1]
            local msgs = redis.call("zrangebyscore", "${invisibleSet}", 0, currentTime);
            if (table.getn(msgs) > 0) then
                if (unpack) then
                    redis.call("lpush", "${workQueue}", unpack(msgs));
                else
                    if (table.unpack) then
                        redis.call("lpush", "${workQueue}", table.unpack(msgs));
                    end
                end
                return redis.call("zremrangebyscore", "${invisibleSet}", 0, currentTime);
            end
        `, 0, Date.now());
    }
}

export function lockingQueueName(workQueue:string) {
    return `${workQueue}:lock`;
}


interface ProcessJobFunc {
    (job: string|null) : void;
}
/**
 * Dequeue a job for processing and mark it invisible for a duration.
 * @param redis 
 * @param workQueue 
 * @param processingSet 
 * @param invisibilityTimeoutMs 
 * @returns 
 */
export function workerFn(redis: Redis, workQueue:string,
        processingSet:string, invisibilityTimeoutMs:number) {

    const lockingQueue = lockingQueueName(workQueue);
    const expireVisibility = expireVisibilityFn(redis, processingSet, workQueue);

    // To mock error every N attempts
    let loopCount = 0;

    const dequeueJob = async () : Promise<string|null> => {

        // Move tasks marked as invisible whose timeout has elapsed.
        expireVisibility();

        if (
                // Execute tasks from the locked queue
                !(await redis.llen(lockingQueue)) &&
                // Try popping a task for locking without blocking
                !(await redis.rpoplpush(workQueue, lockingQueue)) &&
                // Pop a task for locking or block if queue is empty
                !(await redis.brpoplpush(workQueue, lockingQueue, 15)))
            return null;

        // TODO: remove the error simulation.
        // Simulate error: Every 100th time the program fails midway
        if ((loopCount = ++loopCount % 100) === 0) {
            console.error("MOCK - Error occurred while marking order as invisible. It will retried immediately.")
            return null;
        }

        // Mark message as invisible until timeout
        const messageToProcess = await redis.eval(`
            local msg = redis.call("rpop", "${lockingQueue}");
            if (msg) then
                redis.call("zadd", "${processingSet}", ARGV[1], msg);
            end
            return msg;
        `, 0, Date.now() + invisibilityTimeoutMs);

        return messageToProcess;
    };

    return async (process:ProcessJobFunc) => {
        while (true) {
            try {
                await process(await dequeueJob());
            } catch (err) {
                console.error(err);
            }
        }
    };
}

export async function promoteUntil(redis: Redis,
        sourceZset:string, predicateZset:string, targetZset:string, sortByZset=predicateZset) {

    if (sortByZset != sourceZset && sortByZset != predicateZset) {
        throw new Error("sortByZset should be either sourceZset or predicateZset");
    }

    const releasedOrdersCmd = `
    local sourceZset, predicateZset, targetZset, sortByZset = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
    local count = 0
    repeat
        local msgs = redis.call("zrange", sourceZset, 0, 0)
        if (#msgs > 0) then
            local rankInPredSet = redis.call("zrank", predicateZset, msgs[1])
            if (rankInPredSet) then
                local itemScore = redis.call("zpopmin", sourceZset)
                local srcItem, srcScore = itemScore[1], itemScore[2]
                if (targetZset == sourceZset) then
                    redis.call("zadd", targetZset, srcScore, srcItem)
                else
                    local scoreInPredSet = redis.call("zscore", predicateZset, srcItem)
                    redis.call("zadd", targetZset, scoreInPredSet, srcItem)
                end
                redis.call("zremrangebyrank", predicateZset, rankInPredSet, rankInPredSet)
                count = count + 1
            else
                -- Block until the order in the front of the account orders queue is terminated
                break
            end
        else
            -- Nothing in the account orders queue
            break
        end
    until(false)
    return count
    `;
    
    return redis.eval(releasedOrdersCmd, 0, sourceZset, predicateZset, targetZset, sortByZset).catch(err => console.error(err));
}

/**
 * Get lengths of lists, sets and zsets. Returns 0 for objects not existing or of other Redis types.
 * @param redis 
 * @param listNames 
 * @returns Lengths of lists, sets and zsets. 0 for other types.
 */
 export async function queueSizes(redis: Redis, ...listNames:string[]) :Promise<number[]> {
    // Figure out the type of collection names
    const getTypesCmds = listNames.map((listName) => `redis.call("type", "${listName}")`).join(",");
    const listTypes = await redis.eval(`return { ${getTypesCmds} }`, 0) as string[];

    // Compile commands to learn length of given collections
    const luaRedisLenCmds = listNames
    .map((listName, i) => {
        const listType = listTypes[i];
        switch(listType) {
            case "list": return {listName, listType, lenCall: `redis.call("llen", "${listName}")`};
            case "set":  return {listName, listType, lenCall: `redis.call("scard", "${listName}")`};
            case "zset": return {listName, listType, lenCall: `redis.call("zcard", "${listName}")`};
            default: return {listName, listType, lenCall: '0'};
        }
    });

    // Get lengths of list, set and zset
    let cmds = "", firstCmd = true;
    for (let lenCall of luaRedisLenCmds) {
        if (!firstCmd) cmds += ",";
        cmds += (await lenCall).lenCall;
        firstCmd = false;
    }
    return redis.eval(`return {${cmds}}`, 0);
}