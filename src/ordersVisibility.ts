import { ORDERS_INVISIBLE, ORDERS_LOOP, ORDERS_PORTFOLIO_UPDATE } from "./common/constants";
import { expireVisibilityFn, lockingQueueName, queueSizes } from "./common/queueUtils";
import { LocalConnection } from "./common/redisConnection";

/**
 * Expiring order visibility frequently is not strictly required. It is done while dequeuing the orders from work queue.
 */

const redis = new LocalConnection().newConnection();
const INVISIBILITY_CHECK_FREQ_MS = 1000;

let prev_total = -1;

const expireVisiblity = expireVisibilityFn(redis, ORDERS_INVISIBLE, ORDERS_LOOP);
setInterval(async () => {
    await expireVisiblity();

    const ORDER_LOCK = lockingQueueName(ORDERS_LOOP);

    // Logging count of messages in the flow
    const counts = await queueSizes(redis, ORDERS_LOOP, ORDER_LOCK, ORDERS_INVISIBLE, ORDERS_PORTFOLIO_UPDATE);
    const total = counts[0] + counts[1] + counts[2];
    if (total != prev_total) {
        console.log(`TOTAL=${total} = `+
        `${ORDERS_LOOP}: ${counts[0]} + ${ORDER_LOCK}: ${counts[1]} + ${ORDERS_INVISIBLE}: ${counts[2]}` + 
        `; ${ORDERS_PORTFOLIO_UPDATE} = ${counts[3]}`);
    }
    
    prev_total = total;

}, INVISIBILITY_CHECK_FREQ_MS);

0;