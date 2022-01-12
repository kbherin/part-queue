import { Redis } from "ioredis";
import { INVISIBILITY_CHECK_FREQ_MS, PRIORITY_RETRY_QUEUE_EVICT_MS, RETRY_ON_FAILURE } from "./constants";

export interface DequeueJob {job:string, ts:number}

/**
 * Expire visibility of jobs in 'processing' set based on previously supplied timeout.
 * @param redis
 * @param invisibleSet 
 * @param workQueue 
 * @returns 
 */
export function expireVisibilityFn(redis: Redis, invisibleSet:string, workQueue:string, atBack = true) {
    return async () => {
        // @ts-ignore
        const count = await redis.expireInvisibility(invisibleSet, workQueue, Date.now(), atBack ? "1" : "0");
        if (count) {
            console.debug(`Evicted ${count} records from invisible set to the ${atBack ? 'back' : 'front'} of main queue`);
        }
        return count;
    }
}

export function lockingQueueName(workQueue:string) {
    return `${workQueue}:lock`;
}
export function invisibleSetName(workQueue:string) {
    return `${workQueue}:invisible`;
}
export function priorityRetrySetName(workQueue:string) {
    return `${workQueue}:priorityretry`;
}


interface ProcessJobFunc {
    (job: DequeueJob) : Promise<void>;
}
/**
 * Dequeue a job for processing and mark it invisible for a duration.
 * @param redis 
 * @param jobsQueue 
 * @param invisibleSet 
 * @param invisibilityTimeoutMs 
 * @returns 
 */
export function workerFn(redis: Redis, jobsQueue:string,
        invisibilityTimeoutMs:number=INVISIBILITY_CHECK_FREQ_MS, retryCount=RETRY_ON_FAILURE) {

    const lockingQueue = lockingQueueName(jobsQueue),
          invisibleSet = invisibleSetName(jobsQueue),
          priorityRetrySet = priorityRetrySetName(jobsQueue);
    const expireVisibility = expireVisibilityFn(redis, invisibleSet, jobsQueue);
    const evictPriorityRetry = expireVisibilityFn(redis, priorityRetrySet, jobsQueue, false);

    // To mock error every N attempts
    let loopCount = 0;

    const dequeueJob = async () : Promise<DequeueJob|null> => {

        // Move tasks marked as invisible whose timeout has elapsed.
        evictPriorityRetry();
        expireVisibility();

        if (
                // Execute tasks from the locked queue
                !(await redis.llen(lockingQueue)) &&
                // Try popping a task for locking without blocking
                !(await redis.rpoplpush(jobsQueue, lockingQueue)) &&
                // Pop a task for locking or block if queue is empty
                !(await redis.brpoplpush(jobsQueue, lockingQueue, 15)))
            return null;

        // TODO: remove the error simulation.
        // Simulate error: Every 100th time the program fails midway
        if ((loopCount = ++loopCount % 100) === 0) {
            throw new Error("MOCK - Error occurred while marking order as invisible. It will be retried immediately.");
        }

        // Mark message as invisible until timeout
        // @ts-ignore
        const messageTs = await redis.markInvisible(lockingQueue, invisibleSet, invisibilityTimeoutMs);

        if (!messageTs[0]) {
            return null;
        }

        return {job: messageTs[0], ts: Number.parseInt(messageTs[1])};
    };

    return async (process:ProcessJobFunc) => {
        while (true) {
            let retryRemaining = 1 + retryCount;
            try {
                // Job can be null if queue is empty and waiting times out
                const job = await dequeueJob();

                while (job && retryRemaining-- > 0) {
                    try {
                        await process(job);
                        await redis.zrem(invisibleSet, job.job);
                        retryRemaining = 0;
                    } catch (err) {
                        console.error(err, "... Retrying");
                    }
                }
            } catch (err) {
                console.error("Exception occurred while dequeuing job.", err);
            }
        }
    };
}

/**
 * Moves tasks from sourceZset to targetZset if it exists in predicateZset.
 * Score in target set is by the score in predicate if target is different from source.
 * Otherwise it is scored by the score in source set.
 * @param redis 
 * @param sourceZset 
 * @param predicateZset 
 * @param targetZset 
 * @returns 
 */
export async function promoteUntil(redis: Redis,
        sourceZset:string, predicateZset:string, targetZset:string) {

    // @ts-ignore
    return redis.promoteUntil(sourceZset, predicateZset, targetZset)
        .catch((err:any) => console.error(err));
}


/**
 * Mark a job as complete and reorder it in the grouped jobs queue according to completion timestamp, for next phase of processing. 
 * @param redis
 * @param jobSubmitted 
 * @param jobCompleted 
 * @param completionTimeMs 
 * @param groupedJobsQueue 
 * @param completedJobsSet 
 */
export async function markGroupJobComplete(redis:Redis, jobSubmitted:DequeueJob, jobCompleted:string, completionTimeMs:number,
    groupedJobsQueue:string, completedJobsSet:string, jobsQueue:string) {

    return redis.pipeline()
        // Replace incomplete order with completed order in the account's orders queue
        .zrem(groupedJobsQueue, jobSubmitted.job)
        .zadd(groupedJobsQueue, jobSubmitted.ts, jobCompleted)
        // Mark the order as complete and record order execution time.
        // Order execution time will be used to reorder the completed orders for portfolio update.
        .zadd(completedJobsSet, completionTimeMs, jobCompleted)
        .zrem(invisibleSetName(jobsQueue), jobSubmitted.job)
        .exec();
}

/**
* To mark a job incomplete and reinsert at the tail of the grouped jobs queue and main jobs queue.
* @param redis 
* @param jobSubmitted 
* @param groupedJobsQueue 
* @param jobsQueue 
*/
export async function markGroupJobIncomplete(redis:Redis, jobSubmitted:DequeueJob, groupedJobsQueue:string, jobsQueue:string) {

    return redis.pipeline()
        // Update last checked time in account's orders queue. This releases completed orders stuck behind an incomplete order.
        .zadd(groupedJobsQueue, jobSubmitted.ts, jobSubmitted.job)
        // Add to retry-after set
        .zadd(priorityRetrySetName(jobsQueue), Date.now()+PRIORITY_RETRY_QUEUE_EVICT_MS, jobSubmitted.job)
        // And cycle the order to the back of orders loop.
        // .lpush(jobsQueue, jobSubmitted.job)
        .zrem(invisibleSetName(jobsQueue), jobSubmitted.job)
        .exec();
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

export function expireInvisibilityRedisFn(redis: Redis) :Redis {
    redis.defineCommand("expireInvisibility", {
        numberOfKeys: 0,
        lua: `
            local invisibleSet, workQueue, currentTime, atBack = ARGV[1], ARGV[2], ARGV[3], ARGV[4]
            local msgs = redis.call("zrangebyscore", invisibleSet, 0, currentTime);
            if (table.getn(msgs) > 0) then
                if (unpack) then
                    if (atBack == "0") then
                        redis.call("rpush", workQueue, unpack(msgs));
                    else
                        redis.call("lpush", workQueue, unpack(msgs));
                    end
                else
                    if (table.unpack) then
                        if (atBack == "0") then
                            redis.call("rpush", workQueue, table.unpack(msgs));
                        else
                            redis.call("lpush", workQueue, table.unpack(msgs));
                        end
                    end
                end
                return redis.call("zremrangebyscore", invisibleSet, 0, currentTime);
            end
            return 0
        `
    });
    return redis;
}

export function markInvisibleRedisFn(redis: Redis) :Redis {
    redis.defineCommand('markInvisible', {
        numberOfKeys: 0,
        lua: `
            local lockingQueue, invisibleSet, invisibilityTimeoutMs = ARGV[1], ARGV[2], ARGV[3]
            local msg = redis.call("rpop", lockingQueue);
            local ts = redis.call("time")
            local sec, usec = ts[1], ts[2]
            ts = sec*1000 + usec/1000
            if (msg) then
                redis.call("zadd", invisibleSet, ts+invisibilityTimeoutMs, msg)
            end
            return {msg, ts}
        `
    });
    return redis;
}

export function promoteUntilRedisFn(redis: Redis) :Redis {
    redis.defineCommand('promoteUntil', {
        numberOfKeys: 0,
        lua: `
            local sourceZset, predicateZset, targetZset = ARGV[1], ARGV[2], ARGV[3]
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
        `
    });
    return redis;
}