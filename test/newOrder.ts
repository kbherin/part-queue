import { ORDERS_LOOP } from "../src/common/constants";
import { LocalConnection } from "../src/common/redisConnection";
import { Order } from "../src/model/order";


const redis = (new LocalConnection()).newConnection();

function newOrder(id:string, symbol:string, accountNo:string, type="MARKET", tradeQuantity=Math.random(), indexPrice=Math.random()) {
    return {
        id,
        tradeQuantity,
        indexPrice,
        status: "NEW",
        symbol,
        created: new Date(),
        accountNo
    }
}
function addOpenOrder(order: Order) {
    return redis.lpush(ORDERS_LOOP, JSON.stringify(order));
}
function addExecTs(order: Order, ts?: number) :Order {
    // Order gets terminated(executed/rejected/cancelled) sometime in the next 20 seconds
    order.lastExecuted = new Date(ts || (Date.now() + (Math.random() * 5000)));
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
                accountNo: pickAccount(),
                type: "MARKET"
            }))
            , 10)
    }
}


const tickers = ["GOOG", "MSFT", "AMZN", "HOOD", "F", "IBM", "TSLA", "A"]
const accounts = ["ABC", "BCD", "CDE", "DEF", "EFG"]
function pickTicker() {
    return tickers[Math.floor(Math.random() * tickers.length * 0.99)];
}
function pickAccount() {
    return accounts[Math.floor(Math.random() * accounts.length * 0.99)];
}

// Test1
let currTs = Date.now();
addOpenOrder(addExecTs(newOrder("RDSA001", "RDS-A", "XYZ"), currTs+10000)); // Executes @broker in 10s
addNOrders(100);
addOpenOrder(addExecTs(newOrder( "RDSA002", "RDS-A", "XYZ"), currTs+9500)); // Executes @broker in 9.5s
setTimeout(() => addNOrders(150), 5000);
addOpenOrder(addExecTs(newOrder("RDSA003", "RDS-A", "XYZ"), currTs+14000)); // Executes @broker in 14s
console.log("SHELL should execute in the order RDSA002, RDSA001, RDSA003")

// Test2
addOpenOrder(addExecTs(newOrder("AAL001", "AAL", "XYZ"), currTs+2001)); // Executes @broker in 2.001s
addNOrders(1000);
addOpenOrder(addExecTs(newOrder("AAL002", "AAL", "XYZ", "LIMIT"), currTs+10000)); // Executes @broker in 10s
setTimeout(() => addNOrders(150), 5000);
addOpenOrder(addExecTs(newOrder("AAL003", "AAL", "XYZ"), currTs+2000)); // Executes @broker in 2s
setTimeout(() => addNOrders(120), 12000);
addOpenOrder(addExecTs(newOrder("AAL004", "AAL", "XYZ"), currTs+2040)); // Executes @broker in 2.04s
console.log("AMERICAN AIRLINES should execute in the order AAL003, AAL001, AAL004, AAL002")