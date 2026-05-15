import "dotenv/config";
import { createClient } from "redis";
import { env } from "./utils/env.js";
import { BALANCES, ORDERBOOKS, ORDERS, type Side, type Fill, type OrderStatus, type OrderType, type RestingOrder } from "./store/exchange-store.js";

export type EngineCommandType =
  | "create_order"
  | "get_depth"
  | "get_user_balance"
  | "get_order"
  | "cancel_order";

export interface EngineRequest {
  correlationId: string;
  responseQueue: string;
  type: EngineCommandType;
  payload: Record<string, unknown>;
}

export interface EngineResponse {
  correlationId: string;
  ok: boolean;
  data?: unknown;
  error?: string;
}

const brokerClient = createClient({ url: env.redisUrl }).on("error", (error) => {
  console.error("Redis broker client error", error);
});

const responseClient = createClient({ url: env.redisUrl }).on("error", (error) => {
  console.error("Redis response client error", error);
});

await Promise.all([brokerClient.connect(), responseClient.connect()]);

// :-)) I added this just to check the flow, remove it when you start
// const DUMMY_SELL_ORDER = {
//   orderId: "dummy-sell-order-1",
//   userId: "dummy-seller",
//   type: "limit",
//   side: "sell",
//   symbol: "BTC",
//   price: 100,
//   qty: 1,
//   filledQty: 0,
//   status: "open",
// };

async function sendResponse(responseQueue: string, response: EngineResponse): Promise<void> {
  await responseClient.lPush(responseQueue, JSON.stringify(response));
}

function handleEngineRequest(message: EngineRequest): unknown {
  /**
   * TODO(student):
   * 1. Check _message.type.
   * 2. Read _message.payload.
   * 3. Call your order book / balance / order logic.
   * 4. Return the data that should go back to the backend.
   *
   * Required message types:
   * - create_order
   * - get_depth
   * - get_user_balance
   * - get_order
   * - cancel_order
   */

  switch (message.type) {
    case "create_order": {
      const order = {
        orderId: crypto.randomUUID(),
        userId: message.payload.userId as string,
        side: message.payload.side as "buy" | "sell",
        type: message.payload.type as OrderType,
        symbol: message.payload.symbol as string,
        price: message.payload.price as number | null,
        qty: message.payload.qty as number,
        filledQty: 0,
        status: "open" as OrderStatus,
        fills: [] as Fill[],
        createdAt: Date.now()
      }
      ORDERS.set(order.orderId, order);

      if (!ORDERBOOKS.has(order.symbol)) {
        ORDERBOOKS.set(order.symbol, { bids: new Map(), asks: new Map() });
      }
      const orderBook = ORDERBOOKS.get(order.symbol)!;

      if (order.side === "buy") {
        while (order.filledQty < order.qty) {
          const lowestAskPrice = Math.min(...orderBook.asks.keys());
          if (lowestAskPrice === Infinity || lowestAskPrice > order.price!) {
            break;
          }
          const askOrders = orderBook.asks.get(lowestAskPrice)!
          const askOrder = askOrders[0]!;

          const fillQty = Math.min(order.qty - order.filledQty, askOrder.qty - askOrder.filledQty);

          order.filledQty += fillQty;
          askOrder.filledQty += fillQty;

          const fill: Fill = {
            fillId: crypto.randomUUID(),
            symbol: order.symbol,
            price: lowestAskPrice,
            qty: fillQty,
            buyOrderId: order.orderId,
            sellOrderId: askOrder.orderId,
            createdAt: Date.now()
          }

          order.fills.push(fill);
          const askOrderRecord = ORDERS.get(askOrder.orderId);
          askOrderRecord?.fills.push(fill);
          if (askOrder.filledQty === askOrder.qty) {
            askOrder.status = "filled";
            askOrders.shift();
            if (askOrders.length === 0) {
              orderBook.asks.delete(lowestAskPrice)
            }
          } else {
            askOrder.status = "partially_filled";
          }
        }

        if (order.filledQty === order.qty) {
          order.status = "filled";
        } else if (order.filledQty > 0) {
          order.status = "partially_filled";
          const restingOrder: RestingOrder = {
            orderId: order.orderId as string,
            userId: order.userId as string,
            side: order.side as "buy" | "sell",
            type: "limit",
            symbol: order.symbol as string,
            price: order.price as number,
            qty: order.qty as number,
            filledQty: order.filledQty as number,
            status: order.status,
            createdAt: order.createdAt as number,
          }
          const existing = orderBook.bids.get(order.price!)
          if (existing) {
            existing.push(restingOrder)
          } else {
            orderBook.bids.set(order.price!, [restingOrder])
          }
        } else {
          const restingOrder: RestingOrder = {
            orderId: order.orderId as string,
            userId: order.userId as string,
            side: order.side as "buy" | "sell",
            type: "limit",
            symbol: order.symbol as string,
            price: order.price as number,
            qty: order.qty as number,
            filledQty: order.filledQty as number,
            status: order.status,
            createdAt: order.createdAt as number,
          }
          const existing = orderBook.bids.get(order.price!)
          if (existing) {
            existing.push(restingOrder)
          } else {
            orderBook.bids.set(order.price!, [restingOrder])
          }
        }
      }
    }

      break
    case "get_depth": {
      const symbol = message.payload.symbol as string;
      if (!ORDERBOOKS.has(symbol)) {
        return { symbol, bids: [], asks: [] };
      }
      const bids = [];
      const orderBook = ORDERBOOKS.get(symbol)!
      for (const [price, orders] of orderBook.bids.entries()) {
        const totalQty = orders.reduce((sum, order) => sum + order.qty, 0);
        bids.push({ price, qty: totalQty });
      }

      const asks = [];
      for (const [price, orders] of orderBook.asks.entries()) {
        const totalQty = orders.reduce((sum, order) => sum + order.qty, 0);
        asks.push({ price, qty: totalQty });
      }

      bids.sort((a, b) => b.price - a.price);
      asks.sort((a, b) => a.price - b.price);
      return { symbol, bids, asks };
    }
    case "get_user_balance": {
      const userId = message.payload.userId as string;
      const balanceExists = BALANCES.has(userId);
      if (!balanceExists) {
        const balance = { USD: { available: 100000, locked: 0 }, BTC: { available: 10, locked: 0 } };
        BALANCES.set(userId, balance);
      }
      return BALANCES.get(userId);
    }

    case "get_order": {
      const orderId = message.payload.orderId as string;
      const userId = message.payload.userId as string;
      const order = ORDERS.get(orderId);
      if (!order) {
        throw new Error("order not found");
      }
      const isOwner = order.userId === userId;
      if (!isOwner) {
        throw new Error("unauthorised");
      }
      return order;
    }
    case "cancel_order": {
      const orderId = message.payload.orderId as string;
      const userId = message.payload.userId as string;
      const order = ORDERS.get(orderId);
      if (!order) {
        throw new Error("order not found");
      }

      const isOwner = order.userId === userId;
      if (!isOwner) {
        throw new Error("unauthorised");
      }
      if (order.status === "filled" || order.status === "cancelled") {
        throw new Error("Order already filled or cancelled.");
      }

      const orderBook = ORDERBOOKS.get(order.symbol);
      order.status = "cancelled";
      if (orderBook) {
        const side = order.side === "buy" ? orderBook.bids : orderBook.asks;
        const priceLevel = side.get(order.price!);
        if (priceLevel) {
          const filtered = priceLevel.filter((o) => o.orderId != orderId);
          side.set(order.price!, filtered);
        }
      }
      return order;
    }
  }

  // just checking the flow, remove this when you start implementing the logic
  // if (message.type === "create_order") {
  //   return {
  //     orderId: crypto.randomUUID(),
  //     status: "filled",
  //     filledQty: DUMMY_SELL_ORDER.qty,
  //     averagePrice: DUMMY_SELL_ORDER.price,
  //     fills: [
  //       {
  //         fillId: crypto.randomUUID(),
  //         symbol: DUMMY_SELL_ORDER.symbol,
  //         price: DUMMY_SELL_ORDER.price,
  //         qty: DUMMY_SELL_ORDER.qty,
  //         buyOrderId: "request-buy-order",
  //         sellOrderId: DUMMY_SELL_ORDER.orderId,
  //       },
  //     ],
  //     note: "Smoke-test response only. Students must replace this with real matching logic.",
  //   };
  // }

  throw new Error("TODO(student): implement this engine request type");
}

console.log(`Engine listening on Redis queue: ${env.incomingQueue}`);

for (; ;) {
  const item = await brokerClient.brPop(env.incomingQueue, 0);
  if (!item) continue;

  let message: EngineRequest;

  try {
    message = JSON.parse(item.element) as EngineRequest;
  } catch {
    console.error("Skipping invalid broker message");
    continue;
  }

  try {
    const data = handleEngineRequest(message);
    await sendResponse(message.responseQueue, {
      correlationId: message.correlationId,
      ok: true,
      data,
    });
  } catch (error) {
    await sendResponse(message.responseQueue, {
      correlationId: message.correlationId,
      ok: false,
      error: error instanceof Error ? error.message : "engine_error",
    });
  }
}