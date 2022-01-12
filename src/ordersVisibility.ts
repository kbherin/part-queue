import { INVISIBILITY_CHECK_FREQ_MS, ORDERS_LOOP, ORDERS_TERMINATED, ORDERS_PORTFOLIO_UPDATE } from "./common/constants";
import { expireVisibilityFn, invisibleSetName, lockingQueueName, priorityRetrySetName, queueSizes, expireInvisibilityRedisFn } from "./common/queueUtils";
import { LocalConnection } from "./common/redisConnection";

/**
 * Expiring order visibility frequently is not strictly required. It is done while dequeuing the orders from work queue.
 */

const redis = new LocalConnection().newConnection();
expireInvisibilityRedisFn(redis);

const invisibleOrders = invisibleSetName(ORDERS_LOOP);

const priorityRetryOrders = priorityRetrySetName(ORDERS_LOOP);

const expireVisibility = expireVisibilityFn(redis, invisibleOrders, ORDERS_LOOP);
const retryPendingAfterTimeout = expireVisibilityFn(redis, priorityRetryOrders, ORDERS_LOOP, false);

function countsTracker() {
    let prev_totalPending = -1, 
        prev_total = -1;
    
    const lockedOrders = lockingQueueName(ORDERS_LOOP);

    return async () => {
        await retryPendingAfterTimeout();
        await expireVisibility();

        // Logging count of messages in the flow
        const counts = await queueSizes(redis, ORDERS_LOOP, lockedOrders, invisibleOrders, ORDERS_TERMINATED, ORDERS_PORTFOLIO_UPDATE, priorityRetryOrders);
        const totalPending = counts[0] + counts[1] + counts[2] + counts[5];
        const total = totalPending + counts[3] + counts[4];
        if (totalPending !== prev_totalPending || total !== prev_total) {
            console.log(`TOTALPENDING=${totalPending} = `+
            `${ORDERS_LOOP}=${counts[0]} + ${lockedOrders}=${counts[1]} + ${invisibleOrders}=${counts[2]} + ${priorityRetryOrders}=${counts[5]}` + 
            `; ${ORDERS_TERMINATED}=${counts[3]}` +
            `; ${ORDERS_PORTFOLIO_UPDATE}=${counts[4]}`);
        }
        
        prev_totalPending = totalPending;
        prev_total = total;
    }
}

setInterval(countsTracker(), INVISIBILITY_CHECK_FREQ_MS);

0;