import { ORDERS_PORTFOLIO_UPDATE } from "../src/common/constants";
import { LocalConnection } from "../src/common/redisConnection";
import { Order } from "../src/model/order";

const redis = (new LocalConnection()).newConnection();
let test1Seq = new Set(["ID001", "ID002", "ID003"]);

(async () => {
    const orders = await redis.zrange(ORDERS_PORTFOLIO_UPDATE, 0, -1);
    if (orders && orders.length) {
        let prevOrder:Order = JSON.parse(orders[0]);
        if (prevOrder.lastExecuted) {
            prevOrder.lastExecuted = new Date(prevOrder.lastExecuted);
        }

        
        let test1Actual = [] as Order[];
        
        for (let i = 1; i < orders.length; i++) {
            let order:Order = JSON.parse(orders[i]);
            if (order.lastExecuted) {
                order.lastExecuted = new Date(order.lastExecuted);
            }
            if((prevOrder.lastExecuted?.getTime() || 0) > (order.lastExecuted?.getTime() || 0)) {
                console.error(`Wrong order sequence. Expected ${prevOrder.id} before ${order.id}`);
            };

            if (test1Seq.has(order.id)) test1Actual = test1Actual.concat(order);
            prevOrder = order;
        }

        if (test1Actual.map(d => d.id).join(",") !== 'ID002,ID001,ID003') {
            console.error('Orders in portfolio update queue not in the correct sequence. '+
                test1Actual.map(d => `${d.id}@${d.lastExecuted?.getTime()}`).join(','));
        }
    }
})();

console.log("Done")