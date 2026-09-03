import {
  ApiKeyCreds,
  Chain,
  ClobClient,
  OrderType,
  Side,
} from "@polymarket/clob-client-v2";

type MarketMeta = {
  tickSize: string;
  minOrderSize: number;
  negRisk: boolean;
};

export class ClobService {
  private client: ClobClient;

  private marketMetaCache = new Map<
    string,
    {
      meta: MarketMeta;
      expiresAt: number;
    }
  >();

  private readonly META_CACHE_MS = 5 * 60 * 1000;

  constructor(params: {
    host: string;
    chainId: number;
    privateKey: string;
    signatureType: number;
    funderAddress?: string;
    apiKey?: string;
    apiSecret?: string;
    apiPassphrase?: string;
  }) {
    const creds: ApiKeyCreds | undefined =
      params.apiKey && params.apiSecret && params.apiPassphrase
        ? {
            key: params.apiKey,
            secret: params.apiSecret,
            passphrase: params.apiPassphrase,
          }
        : undefined;

    this.client = new ClobClient(
      params.host,
      params.chainId as Chain,
      params.privateKey,
      creds,
      params.signatureType,
      params.funderAddress,
    );
  }

  /**
   * Получаем метаданные рынка.
   */
  private async getMarketMeta(tokenId: string): Promise<MarketMeta> {
    const cached = this.marketMetaCache.get(tokenId);

    if (cached && cached.expiresAt > Date.now()) {
      return cached.meta;
    }

    const book = await this.client.getOrderBook(tokenId);

    const bookAny = book as any;

    const tickSize = String(
      bookAny.tick_size ??
        bookAny.tickSize ??
        "0.01",
    );

    const minOrderSize = Number(
      bookAny.min_order_size ??
        bookAny.minOrderSize ??
        0,
    );

    const negRisk = Boolean(
      bookAny.neg_risk ??
        bookAny.negRisk ??
        false,
    );

    const meta: MarketMeta = {
      tickSize,
      minOrderSize,
      negRisk,
    };

    console.log(
      `[META] token=${tokenId} tickSize=${tickSize} minOrderSize=${minOrderSize} negRisk=${negRisk}`,
    );

    this.marketMetaCache.set(tokenId, {
      meta,
      expiresAt: Date.now() + this.META_CACHE_MS,
    });

    return meta;
  }

  /**
   * Округление цены под tick size.
   *
   * BUY  -> округляем вниз
   * SELL -> округляем вверх
   */
  private roundToTick(
    price: number,
    tickSize: string,
    side: Side,
  ): number {
    const tick = Number(tickSize);

    if (!Number.isFinite(tick) || tick <= 0) {
      return price;
    }

    const factor = 1 / tick;
    const raw = price * factor;

    const rounded =
      side === Side.BUY
        ? Math.floor(raw)
        : Math.ceil(raw);

    return rounded / factor;
  }

  /**
   * Получить лучший ask.
   */
  async getBestAsk(tokenId: string): Promise<number | null> {
    const book = await this.client.getOrderBook(tokenId);

    const asks = (book as any)?.asks;

    if (!Array.isArray(asks) || asks.length === 0) {
      return null;
    }

    const prices = asks
      .map((x: any) => Number(x.price))
      .filter((x: number) => Number.isFinite(x));

    if (prices.length === 0) {
      return null;
    }

    return Math.min(...prices);
  }

  /**
   * Точный GTC LIMIT ордер.
   *
   * ВАЖНО:
   * - BUY 0.98 -> остаётся 0.98
   * - SELL 0.999 -> 0.999 если tickSize позволяет
   * - если tickSize=0.01, SELL 0.999 становится 0.99
   * - никогда не отправляем цену 1.00
   */
  async placeExactGtcLimitOrder(params: {
    tokenId: string;
    side: Side;
    price: number;
    size: number;
  }): Promise<{
    status: string;
    orderId?: string;
    filledSize?: string;
    filledUsdc?: string;
  }> {
    const {
      tokenId,
      side,
      size,
    } = params;

    const meta = await this.getMarketMeta(tokenId);

    if (
      !Number.isFinite(params.price) ||
      params.price <= 0 ||
      params.price >= 1
    ) {
      throw new Error(
        `Invalid limit price ${params.price}`,
      );
    }

    if (
      !Number.isFinite(size) ||
      size <= 0
    ) {
      throw new Error(
        `Invalid order size ${size}`,
      );
    }

    if (size < meta.minOrderSize) {
      throw new Error(
        `Order size ${size} is below the market minimum ${meta.minOrderSize} — not submitted.`,
      );
    }

    /**
     * Сначала округляем цену под tickSize.
     */
    const roundedPrice = this.roundToTick(
      params.price,
      meta.tickSize,
      side,
    );

    /**
     * Polymarket не принимает 1.00.
     *
     * Поэтому:
     * 0.999 + tickSize 0.01
     * -> 1.00
     * -> принудительно 0.99
     */
    const price = Math.min(
      0.99,
      Math.max(0.01, roundedPrice),
    );

    if (price !== params.price) {
      console.warn(
        `[PRICE ADJUST] requested=${params.price} tickSize=${meta.tickSize} rounded=${roundedPrice} final=${price}`,
      );
    }

    /**
     * Дополнительная защита.
     */
    if (
      !Number.isFinite(price) ||
      price <= 0 ||
      price >= 1
    ) {
      throw new Error(
        `Invalid final price ${price} (requested=${params.price}, tickSize=${meta.tickSize})`,
      );
    }

    console.log(
      `[ORDER] ${side === Side.BUY ? "BUY" : "SELL"} token=${tokenId} price=${price} size=${size} tickSize=${meta.tickSize}`,
    );

    const resp =
      await this.client.createAndPostOrder(
        {
          tokenID: tokenId,
          price,
          size,
          side,
        },
        {
          tickSize: meta.tickSize as any,
          negRisk: meta.negRisk,
        },
        OrderType.GTC,
      );

    const respAny = resp as any;

    console.log(
      `[ORDER RESPONSE]`,
      JSON.stringify(respAny),
    );

    if (respAny.success !== true) {
      const reason =
        respAny.error ||
        respAny.errorMsg ||
        `CLOB rejected the order (status: ${respAny.status})`;

      throw new Error(reason);
    }

    return {
      status: String(
        respAny.status ?? "accepted",
      ),
      orderId:
        respAny.orderID ??
        respAny.orderId,
      filledSize:
        respAny.makingAmount ??
        respAny.filledSize,
      filledUsdc:
        respAny.takingAmount ??
        respAny.filledUsdc,
    };
  }

  /**
   * Получить информацию об ордере.
   */
  async getOrder(orderId: string): Promise<any> {
    return await this.client.getOrder(orderId);
  }

  /**
   * Отменить ордер.
   */
  async cancelOrder(orderId: string): Promise<any> {
    return await this.client.cancelOrder({
      orderID: orderId,
    });
  }

  /**
   * Отменить все ордера.
   */
  async cancelAllOrders(): Promise<any> {
    return await this.client.cancelAll();
  }

  /**
   * Получить баланс/allowance.
   */
  async getBalanceAllowance(params?: any): Promise<any> {
    return await (this.client as any).getBalanceAllowance(
      params,
    );
  }
}