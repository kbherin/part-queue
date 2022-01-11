export const
    ORDERS_LOOP = "orders",
    ORDERS_TERMINATED = `${ORDERS_LOOP}:terminated`,
    ORDERS_PORTFOLIO_UPDATE = `${ORDERS_LOOP}:portfolioupdate`;

export const INVISIBILITY_CHECK_FREQ_MS = Number.parseInt('' + process.env.INVISIBILITY_CHECK_FREQ_MS) || 5000,
    PRIORITY_RETRY_QUEUE_EVICT_MS = Number.parseInt('' + process.env.PRIORITY_RETRY_QUEUE_EVICT_MS) || 5000,
    RETRY_ON_FAILURE = 3;
