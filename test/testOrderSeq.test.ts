import { Redis } from "ioredis";
import { exit } from "process";
import { ORDERS_PORTFOLIO_UPDATE } from "../src/common/constants";
import { LocalConnection } from "../src/common/redisConnection";
import { Order } from "../src/model/order";

const redis = (new LocalConnection()).newConnection();
const seqTests = new Map<string, Array<string>>([
    ["test1Seq", "RDSA002, RDSA001, RDSA003".split(", ")],
    ["test2Seq", "AAL003, AAL001, AAL004, AAL002".split(", ")]
]);

async function verifyOrdersSeq(redis: Redis, seqTests: Map<string, Array<string>>) {
    const orders = await redis.zrange(ORDERS_PORTFOLIO_UPDATE, 0, -1);
    if (orders && orders.length) {
        let prevOrder:Order = JSON.parse(orders[0]);
        if (prevOrder.lastExecuted) {
            prevOrder.lastExecuted = new Date(prevOrder.lastExecuted);
        }

        
        let seqTestsResult = new Map<string, Array<Order>>();
        
        for (let i = 1; i < orders.length; i++) {
            let order:Order = JSON.parse(orders[i]);
            if (order.lastExecuted) {
                order.lastExecuted = new Date(order.lastExecuted);
            }
            if((prevOrder.lastExecuted?.getTime() || 0) > (order.lastExecuted?.getTime() || 0)) {
                console.error(`Wrong order sequence. Expected ${prevOrder.id} before ${order.id}`);
            };

            seqTests.forEach((seq,testName) => {
                if (seq.indexOf(order.id) > -1) seqTestsResult.set(testName, (seqTestsResult.get(testName) || []).concat(order));
            })
            prevOrder = order;
        }

        let errorsCount = 0;
        seqTestsResult.forEach((seq, testName) => {
            let expected = seqTests.get(testName)?.join(", ");
            let actual = seq.map(d => d.id).join(", ");
            if (actual !== expected) {
                console.error(`${testName}: Orders in portfolio update queue not in the correct sequence. Expected: ${expected}; Actual: + ${actual}`);
                errorsCount++;
            }
            console.log(`Execution for ${testName}: ${seq.map(d => d.id+'@'+d.executedDate?.substr(11)).join(", ")}`);
        })
        console.log("Tests complete. Number of errors in execution sequence: " + errorsCount);
        exit(0);
    }
}

verifyOrdersSeq(redis, seqTests);

console.log("Verifying execution sequence of orders\n");