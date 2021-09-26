export type Status = "NEW" | "FILLED" | "PARTIALLY_FILLED" | "REJECTED" | "PENDING" | "CANCELED" | "QUEUED";

export interface Order {
    id: string,
    orderNo?: string,
    type?: string,
    side?: string,
    status?: string,
    symbol?: string,
    averagePrice?: number,
    totalOrderAmount?: number,
    cumulativeQuantity?: number,
    quantity?: number,
    fees?: number,
    createdBy?: string,
    userID?: string,
    accountID?: string,
    accountNo?: string,
    username?: string,
    created?: Date,
    statusMessage?: undefined,
    amountCash?: number,
    orderExpires?: Date,
    partnerCode?: string,
    subscriptionType?: string,
    portfolioStatus?: boolean,
    category?: string,
    stackID?: string,
    addToQueue?: boolean,
    lastExecuted?: Date,
    executedDate?: string,
    investmentAmount?: number,
    indexPrice?: number,
    fullname?: string,
    sellOption?: string,
    tradeFees?: number,
    tradeAmount?: number,
    tradeQuantity?: number,
    remainingQty?: number,
    sellExecxuted?: boolean,
    source?: string
}
