import { INVISIBILITY_CHECK_FREQ_MS, ORDERS_LOOP, ORDERS_PORTFOLIO_UPDATE } from "./common/constants";
import { expireVisibilityFn, invisibleSetName, lockingQueueName, queueSizes } from "./common/queueUtils";
import { LocalConnection } from "./common/redisConnection";

/**
 * Expiring order visibility frequently is not strictly required. It is done while dequeuing the orders from work queue.
 */

const redis = new LocalConnection().newConnection();

const invisibleOrders = invisibleSetName(ORDERS_LOOP);

const expireVisibility = expireVisibilityFn(redis, invisibleOrders, ORDERS_LOOP);
function countsTracker() {
    let prev_totalPending = -1, 
        prev_total = -1;
    
    const lockedOrders = lockingQueueName(ORDERS_LOOP);

    return async () => {
        await expireVisibility();

        // Logging count of messages in the flow
        const counts = await queueSizes(redis, ORDERS_LOOP, lockedOrders, invisibleOrders, ORDERS_PORTFOLIO_UPDATE);
        const totalPending = counts[0] + counts[1] + counts[2];
        const total = totalPending + counts[3];
        if (totalPending !== prev_totalPending || total !== prev_total) {
            console.log(`TOTAL=${totalPending} = `+
            `${ORDERS_LOOP}=${counts[0]} + ${lockedOrders}=${counts[1]} + ${invisibleOrders}=${counts[2]}` + 
            `; ${ORDERS_PORTFOLIO_UPDATE}=${counts[3]}`);
        }
        
        prev_totalPending = totalPending;
        prev_total = total;
    }
}

setInterval(countsTracker(), INVISIBILITY_CHECK_FREQ_MS);

0;