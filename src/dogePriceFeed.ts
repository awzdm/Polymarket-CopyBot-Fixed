/** Dogecoin: фид цены через PolyBolt (см. polyboltFeed.ts). Раньше был RTDS crypto_prices_twap_thirty — он больше не работает. */
import { CoinFeed } from "./polyboltFeed.js";

export const dogePriceFeed = new CoinFeed("dogeusd", "dogePriceFeed");
