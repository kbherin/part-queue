import { ORDERS_LOOP } from "../src/common/constants";
import { LocalConnection } from "../src/common/redisConnection";
import { Order } from "../src/model/order";


const redis = (new LocalConnection()).newConnection();

function addOpenOrder(order: Order) {
    return redis.lpush(ORDERS_LOOP, JSON.stringify(order));
}
function addExecTs(order: Order, ts?: number) :Order {
    // Order got terminated(executed/rejected/cancelled) sometime in the last 20 seconds
    order.lastExecuted = new Date(Date.now() + (ts || (Math.random() * 10000)));
    order.executedDate = order.lastExecuted.toISOString();
    return order;
}
function addNOrders(n: number) {
    for (let i = 0; i < n; i++) {
        setTimeout(() => 
            addOpenOrder(addExecTs({
                id: "ID" + Date.now() + Math.floor(Math.random()*1000),
                tradeQuantity: Math.random(),
                indexPrice: Math.random(),
                status: "NEW",
                symbol: pickTicker(),
                created: new Date(),
                accountNo: pickAccount()
            }))
            , 10)
    }
}


const tickers = ["GOOG", "MSFT", "AMZN", "HOOD", "F", "IBM", "TSLA"]
const accounts = ["ABC", "BCD", "CDE", "DEF", "EFG"]
function pickTicker() {
    return tickers[Math.floor(Math.random() * tickers.length * 0.99)];
}
function pickAccount() {
    return accounts[Math.floor(Math.random() * 0.99)];
}

const xyz_ord1 = addExecTs({
    id: "ID001",
    tradeQuantity: Math.random(),
    indexPrice: Math.random(),
    status: "NEW",
    symbol: "SHELL",
    created: new Date(),
    accountNo: "XYZ"
}, 10000);  // Executes in 10s
addOpenOrder(xyz_ord1);
console.log(xyz_ord1)

addNOrders(100);

const xyz_ord2 = addExecTs({
    id: "ID002",
    tradeQuantity: Math.random(),
    indexPrice: Math.random(),
    status: "NEW",
    symbol: "SHELL",
    created: new Date(),
    accountNo: "XYZ"
}, 9500); // Executes in 9.5s
addOpenOrder(xyz_ord2);
console.log(xyz_ord2);

addNOrders(100);

const xyz_ord3:Order = addExecTs({
    id: "ID003",
    tradeQuantity: Math.random(),
    indexPrice: Math.random(),
    status: "NEW",
    symbol: "SHELL",
    created: new Date(),
    accountNo: "XYZ"
}, 14000); // Executes in 14s
addOpenOrder(xyz_ord3);
console.log(xyz_ord3);