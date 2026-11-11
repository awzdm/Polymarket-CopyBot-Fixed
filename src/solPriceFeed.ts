/** Solana: фид цены через PolyBolt (см. polyboltFeed.ts). Раньше был RTDS crypto_prices_twap_thirty — он больше не работает. */
import { CoinFeed } from "./polyboltFeed.js";

export const solPriceFeed = new CoinFeed("solusd", "solPriceFeed");
