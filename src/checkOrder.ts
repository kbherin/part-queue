import { LocalConnection } from "./common/redisConnection";
import { Order, Status } from "./model/order";
import { ORDERS_LOOP, ORDERS_INVISIBLE, ORDERS_PORTFOLIO_UPDATE, ORDERS_ACCOUNT_LOOP, ORDERS_TERMINATED} from "./common/constants";
import { workerFn, expireVisibilityFn, promoteUntil } from "./common/queueUtils";

const redis = (new LocalConnection()).newConnection();
const INVISIBILITY_TIMEOUT_MS = 1 * 1000;

const STATUSES: Status[] = ["FILLED", "CANCELED", "NEW", "PARTIALLY_FILLED", "PENDING", "QUEUED", "REJECTED"];
const TERMINAL_STATUSES: Status[] = ["FILLED", "CANCELED", "REJECTED"];

interface CheckOrderFunc {
    (order: Order) : Promise<Order>;
}

// MOCK: Call broker API for exectution status of the order
// Assuming that a broker API call takes 100ms and errors out in 1% of the API calls.
async function checkOrderStatusAtBroker(order :Order) : Promise<Order> {
    // Call Mock DW API here
    return new Promise<Order>((resolve, reject) => {
        setTimeout(() => {
            // Simulate API erroring in 1% of the API calls
            if (Math.floor(Math.random()*100)) {
                // order.status = STATUSES[Math.floor(Math.random() * STATUSES.length * 0.99)]
                // if (isOrderDone(order)) {
                //     // Order got terminated(executed/rejected/cancelled) sometime in the last 10 seconds
                //     order.lastExecuted = new Date(Date.now() - Math.random() * 10000);
                //     order.executedDate = order.lastExecuted.toISOString();
                // }

                if (order.lastExecuted) {
                    order.lastExecuted = new Date(order.lastExecuted);
                }
                if ((order.lastExecuted?.getTime() || Number.MAX_SAFE_INTEGER) < Date.now()) {
                    order.status = "FILLED";
                } else {
                    order.status = "PENDING";
                }
                resolve(order);
            } else {
                reject(new Error(`MOCK - API error while checking status for ${order.id}. Order will be re-processed after visibility timeout.`));
            }
        }, 100); // API takes 100ms to return status
    });
}

interface OrderDoneFunc {
    (order: Order|null) : boolean;
}
// Tests completion of order execution.
function isOrderDone(order: Order|null) :boolean {
    return !!order && (order.status === "FILLED" || order.status === "REJECTED" || order.status === "CANCELED");
}

function processOrderFn(checkOrderStatus: CheckOrderFunc, isOrderDone: OrderDoneFunc) {

    return async function (orderStr: string|null) {
        if (!orderStr) return;

        const order = JSON.parse(orderStr);
        // Call broker API to check order status
        let checkedOrder = null;
        try {
            checkedOrder = await checkOrderStatus(order);
        } catch (err:any) {
            console.error(err.message || err);
            return;
        }

        const accountOrders = `${ORDERS_ACCOUNT_LOOP}:${checkedOrder.accountNo}`;
        const nextActionPipe = redis.pipeline();
        nextActionPipe.zrem(ORDERS_INVISIBLE, orderStr);
        if (checkedOrder && checkedOrder.lastExecuted && isOrderDone(checkedOrder)) {
            console.log(`Order ${order.id} complete`);
            const checkedOrderStr = JSON.stringify(checkedOrder);
            nextActionPipe
                    // Replace incomplete order with completed order in the account's orders queue
                    .zrem(accountOrders, orderStr)
                    .zadd(accountOrders, Date.now(), checkedOrderStr)
                    // Mark the order as complete and record order execution time.
                    // Order execution time will be used to reorder the completed orders for portfolio update.
                    .zadd(ORDERS_TERMINATED, checkedOrder.lastExecuted?.getTime() || Date.now(), checkedOrderStr);
        } else {
            console.log(`Order ${order.id} incomplete`);
            nextActionPipe
                    // Update last checked time in account's orders queue. This releases completed orders stuck behind an incomplete order.
                    .zadd(accountOrders, Date.now(), orderStr)
                    // And cycle the order to the back of orders loop.
                    .lpush(ORDERS_LOOP, orderStr);
        }
        await nextActionPipe.exec();

        const releasedOrdersCount = await promoteUntil(redis, accountOrders, ORDERS_TERMINATED, ORDERS_PORTFOLIO_UPDATE);
        console.log(releasedOrdersCount + " orders ready for updating portfolios");
    }
}


const ordersWorker = workerFn(redis, ORDERS_LOOP, ORDERS_INVISIBLE, INVISIBILITY_TIMEOUT_MS);
const processOrder = processOrderFn(checkOrderStatusAtBroker, isOrderDone);
(async () => await ordersWorker(processOrder))();

0;