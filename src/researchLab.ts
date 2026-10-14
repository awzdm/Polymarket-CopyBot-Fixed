/**
 * LAB v2.4 — исследовательский парсер идей (НЕ торгует, только собирает статистику).
 * Работает отдельным процессом, свой файл состояния (research-lab-state.json),
 * свои команды в Telegram (все начинаются со слова "лаб").
 *
 * ЧТО НОВОГО В v2.4 (всё добавлено, старое не тронуто; бот не затронут):
 *  - «Binance» (лаб бинанс): к каждому входу профилей бота пишем, как далеко от цены открытия монета на Binance
 *    против потока цен бота; отчёт показывает, помогает ли фильтр «Binance уже развернулся — не входим».
 *  - «Лоси» (лаб лоси): к каждому входу пишем признаки (скорость монеты, час, размеры bid/ask, сила z, согласие
 *    профилей, Binance и т.д.); отчёт сравнивает проигравшие входы с выигравшими и показывает последние лоси.
 *  - «Согласие» (лаб согласие): винрейт входа в зависимости от того, сколько профилей дали сигнал ОДНОВРЕМЕННО.
 *  - «Стоп» (лаб стоп): после каждого входа профилей бота пишем, насколько низко опускалась цена токена, и
 *    считаем, что было бы со стоп-уровнями 0.92 ... 0.40: сколько выигрышных входов выбило бы зря, сколько
 *    проигрышных поймали, итоговая прибыль со стопом и без.
 *
 * ЧТО НОВОГО В v2.3: добавлен Профиль 7 «Модель» в BOT_PROFILES (как в боте v7.9); настройки профилей 1, 2 и 6
 *  приведены к настройкам бота (эфир П1 60с/0.30%, солана П2 30с/0.5x, солана П6 60с, XRP П6 z≥2.5).
 *
 * ЧТО НОВОГО В v2.2:
 *  - Идея 10 «Пара BTC+ETH» (команда "лаб пара"): покупаем оба токена, только когда у ОБЕИХ монет
 *    одновременно сработал свой триггер (окно + % движения) в одну сторону и цены токенов в коридоре.
 *    Перебираются разные окна (60/90/120/180с), разные проценты и два коридора цены; окна и проценты
 *    у BTC и ETH могут быть разными. Для сравнения парсер отдельно считает «соло» ту же монету с тем же
 *    триггером (идея psolo), чтобы видно было, помогает ли условие «оба сразу».
 *
 * ЧТО НОВОГО В v2.1 (всё добавлено сверху, старые идеи и их статистика не тронуты):
 *  - Идея 8 «Модель цены» (команда "лаб модель"): для каждой комбинации
 *    сила движения × секунды до закрытия × цена токена считаем реальный винрейт
 *    и сравниваем с ценой. Показывает, где выгодно покупать НЕ только на 0.97-0.98.
 *  - Идея 9 «Пила» (команда "лаб пила"): к каждому входу профилей бота пишем, сколько раз
 *    монета пересекла цену открытия окна за последние 3 минуты. Отчёт показывает,
 *    помогает ли фильтр "не входить при N+ пересечениях" и с какого N.
 *  - В BOT_PROFILES добавлен Профиль 6 «Запас» (как в боте v7.7, с проверкой живого bid).
 *  - Окно Solana в Профиле 1 приведено к боту: 90с (было 120с).
 *  Версия состояния 3: старая статистика (v2) откладывается в отдельный файл, счёт идёт с нуля (резолв-функция исправлена).
 *
 * ЧТО ИЗМЕНИЛОСЬ В v2 (исправление устаревших цен):
 *  В v1 цена ask бралась из памяти: последнее значение, пришедшее по websocket. А websocket
 *  присылает стакан только после сделок, поэтому на тихих рынках ask в памяти мог быть старым.
 *  Из-за этого "дешёвые" входы (0.85-0.90) и арбитраж могли быть ложными.
 *  Теперь:
 *   - Перед записью ЛЮБОГО входа парсер запрашивает живой стакан (REST /book) и пересчитывает
 *     все условия по свежим bid/ask. Если по живым ценам условие не выполняется — вход не пишется.
 *   - Арбитраж считается только по живым стаканам: раз в 5 секунд берём стакан обеих сторон.
 *     Заодно пишем, сколько акций доступно по лучшим ask.
 *   - Парсер считает, как часто цена в памяти отличалась от живой на 0.02 и больше
 *     (это прямой ответ на вопрос "были ли цены устаревшими").
 *  Старый файл состояния (v1) переименовывается в research-lab-state.v1.json, статистика
 *  начинается с нуля — старые цифры по цене/доходности были недостоверными.
 *
 * Что собирает (все входы считаются КАК В БОТЕ: цена входа = лучший ask по живому стакану,
 * итог определяется только официальным резолвом Gamma API, держим до резолва):
 *
 *  1. «Запас» вместо порога — один параметр z на все монеты:
 *     z = движение монеты в сторону токена / (типичное 5-мин движение × √(осталось/300)).
 *     Цена токена 0.97-0.98, как в боте.
 *  2. «Недооценка» — монета уже прошла порог в нужную сторону, а токен стоит 0.85-0.96.
 *  3. «Паника без разворота» — токен был ≥0.97, просел до 0.85-0.95, монета всё ещё за порогом.
 *  4. «Импульс последних секунд» — монета резко прошла N% за последние 15/30с в сторону токена.
 *  5. «Арбитраж Up+Down» — как часто сумма лучших ask обеих сторон ≤ 0.99 / 0.98 / 0.97 / 0.95.
 *  6. «Подтверждение другими монетами» — к каждому входу пишется, сколько ДРУГИХ монет
 *     в этот момент идут в ту же сторону (порог 0.05% и 0.10% от открытия окна).
 *  7. «Согласие профилей» — внутри парсера моделируются 6 профилей бота (настройки ниже,
 *     BOT_PROFILES). Смотрим, как меняется винрейт, когда сигнал дали сразу несколько профилей.
 *  8. «Модель цены» — см. выше.
 *  9. «Пила» — см. выше.
 *
 * ВАЖНО: если меняешь настройки профилей в боте — поменяй их и в BOT_PROFILES ниже.
 *
 * Переменные окружения: LAB_TELEGRAM_BOT_TOKEN и LAB_TELEGRAM_CHAT_ID
 * (если не заданы — берутся TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID).
 * Для нового Railway-сервиса лучше завести ОТДЕЛЬНОГО Telegram-бота (через @BotFather),
 * иначе два процесса с одним токеном будут «воровать» друг у друга команды.
 */

import "dotenv/config";
import * as fs from "fs";
import * as path from "path";
import { discoverCryptoUpDownMarkets, CryptoUpDownMarket } from "./cryptoMarketDiscovery.js";
import { PriceWatcher, PriceUpdate } from "./priceWatcher.js";
import { btcPriceFeed } from "./btcPriceFeed.js";
import { ethPriceFeed } from "./ethPriceFeed.js";
import { solPriceFeed } from "./solPriceFeed.js";
import { xrpPriceFeed } from "./xrpPriceFeed.js";
import { dogePriceFeed } from "./dogePriceFeed.js";
import { VolatilityTracker } from "./volatilityTracker.js";
import { createTelegramNotifier } from "./telegram.js";
import { createLogger } from "./logger.js";

// ─────────────────────────── Общие настройки ───────────────────────────

const STATE_FILE = path.resolve(process.cwd(), "research-lab-state.json");
const STATE_FILE_V1 = path.resolve(process.cwd(), "research-lab-state.v1.json");
const STATE_VERSION = 3;
const AUTOSAVE_INTERVAL_MS = 60 * 1000;

const TARGET_WINDOW_MINUTES = 5;
const TIMEFRAMES_TO_DISCOVER = [{ suffixes: ["up-or-down-5m"], minutes: TARGET_WINDOW_MINUTES }];

const MARKET_REFRESH_MS = 30 * 1000;
const RESOLVE_CHECK_DELAY_SEC = 180;
const RESOLVE_GIVE_UP_MS = 60 * 60 * 1000;
const GAMMA_HOST = "https://gamma-api.polymarket.com";
const CLOB_HOST = process.env.CLOB_HOST ?? "https://clob.polymarket.com";

const VERIFY_COOLDOWN_MS = 3 * 1000; // не чаще 1 проверки живым стаканом на токен раз в 3с
const ARB_SAMPLE_MS = 5 * 1000; // замер арбитража живыми стаканами раз в 5с
const FETCH_TIMEOUT_MS = 4 * 1000;
const STALE_DIFF = 0.02; // насколько ask в памяти должен отличаться от живого, чтобы считать его устаревшим

const MIN_TRADES_FOR_TOP = 10; // минимум записей, чтобы комбинация попала в топ
const MAX_SETTLED_REMEMBERED = 3000;

interface CoinPriceFeed {
  getPriceAt(ts: number): number | null;
  getLatestPrice(): number | null;
}
const PRICE_FEEDS: Record<string, CoinPriceFeed> = {
  Bitcoin: btcPriceFeed,
  Ethereum: ethPriceFeed,
  Solana: solPriceFeed,
  XRP: xrpPriceFeed,
  Dogecoin: dogePriceFeed,
};
const INCLUDED_COINS = Object.keys(PRICE_FEEDS);
const COIN_SHORT: Record<string, string> = {
  Bitcoin: "BTC",
  Ethereum: "ETH",
  Solana: "SOL",
  XRP: "XRP",
  Dogecoin: "DOGE",
};

const volTracker = new VolatilityTracker(PRICE_FEEDS);

// ─────────────────────────── Сетки идей ───────────────────────────

// 1. «Запас»
const Z_LEVELS = [1.0, 1.5, 2.0, 2.5, 3.0, 4.0];
const MARGIN_WINDOWS = [30, 60, 90, 120, 180, 300];
const MARGIN_PRICE_LOW = 0.97;
const MARGIN_PRICE_HIGH = 0.98;

// 2. «Недооценка»
const CHEAP_THRESHOLDS = [0.001, 0.0015, 0.002, 0.003];
const CHEAP_WINDOWS = [60, 90, 120, 180, 300];
const CHEAP_BANDS: [number, number][] = [
  [0.85, 0.9],
  [0.9, 0.93],
  [0.93, 0.96],
];

// 3. «Паника без разворота»
const PANIC_THRESHOLDS = [0.001, 0.0015, 0.002, 0.003];
const PANIC_WINDOWS = [30, 60, 90, 120, 180, 300];
const PANIC_BANDS: [number, number][] = [
  [0.85, 0.9],
  [0.9, 0.95],
];
const PANIC_PEAK = 0.97;

// 4. «Импульс последних секунд»
const IMPULSE_LOOKBACKS_SEC = [15, 30];
const IMPULSE_THRESHOLDS = [0.0003, 0.0005, 0.0008, 0.0012];
const IMPULSE_WINDOWS = [60, 120, 180];
const IMPULSE_ASK_MIN = 0.6;
const IMPULSE_ASK_MAX = 0.98;

// 6. «Подтверждение другими монетами»
const CONFIRM_LEVEL_1 = 0.0005; // 0.05%
const CONFIRM_LEVEL_2 = 0.001; // 0.10%

// 8. «Модель цены»: сила движения (z) × секунды до закрытия × цена токена (ask).
// Корзины НЕ пересекаются: в каждый момент ситуация попадает ровно в одну корзину по каждой оси.
const MODEL_Z_BANDS: [number, number][] = [
  [0.5, 1],
  [1, 1.5],
  [1.5, 2],
  [2, 3],
  [3, 99],
];
const MODEL_SEC_BANDS: [number, number][] = [
  [0, 30],
  [30, 60],
  [60, 120],
  [120, 180],
  [180, 240],
];
const MODEL_PRICE_BANDS: [number, number][] = [
  [0.6, 0.7],
  [0.7, 0.8],
  [0.8, 0.85],
  [0.85, 0.9],
  [0.9, 0.93],
  [0.93, 0.96],
  [0.96, 0.97],
  [0.97, 0.98],
  [0.98, 0.99],
];
const MODEL_ASK_MIN = 0.6;
const MODEL_ASK_MAX = 0.99;
const MODEL_MAX_SEC = 240;
const MODEL_MIN_TRADES = 30; // минимум записей на комбинацию для общего топа
const MODEL_MIN_COIN = 15; // минимум записей на комбинацию для отчёта по монете

function zLabel(b: [number, number]): string {
  return `сила ${b[1] >= 99 ? `${b[0]}+` : `${b[0]}-${b[1]}`}`;
}
function secLabel(b: [number, number]): string {
  return b[0] === 0 ? `до ${b[1]}с` : `${b[0]}-${b[1]}с`;
}
function priceLabel(b: [number, number]): string {
  return `${b[0].toFixed(2)}-${b[1].toFixed(2)}`;
}

// 9. «Пила»: сколько раз монета пересекла цену открытия окна за последние N секунд до входа
const SAW_LOOKBACK_SEC = 180;
const SAW_STEP_SEC = 5;
const SAW_BUCKETS = [0, 1, 2, 3]; // 3 = «3 и больше»

function sawLabel(n: number): string {
  return n >= 3 ? "3+" : String(n);
}

// 10. «Пара BTC+ETH»: оба триггера должны сработать одновременно, в одну сторону.
const PAIR_COINS = ["Bitcoin", "Ethereum"]; // порядок важен: [0] = BTC, [1] = ETH
const PAIR_WINDOWS = [60, 90, 120, 180];
const PAIR_MOVES = [0.001, 0.0015, 0.002, 0.003]; // 0.10% / 0.15% / 0.20% / 0.30%
const PAIR_BANDS: [number, number][] = [
  [0.95, 0.97],
  [0.97, 0.98],
];
const PAIR_MIN_TRADES = 10; // минимум пар на комбинацию для топа (снижено с 30, чтобы топ уже показывал данные)
const PAIR_PRUNE_MS = 10 * 60 * 1000;

// 11-14. Binance, признаки лосей, согласие профилей, стопы
const BINANCE_SYMBOLS: Record<string, string> = {
  Bitcoin: "BTCUSDT",
  Ethereum: "ETHUSDT",
  Solana: "SOLUSDT",
  XRP: "XRPUSDT",
  Dogecoin: "DOGEUSDT",
};
const BINANCE_HOSTS = ["https://data-api.binance.vision", "https://api.binance.com"];
const BINANCE_POLL_MS = 1000;
const BINANCE_KEEP_MS = 15 * 60 * 1000;
const BINANCE_MATCH_MS = 4000; // допуск при поиске цены на нужный момент
const BINANCE_FRESH_MS = 5000; // последняя цена не старше этого

/** Цена монет с Binance: опрос REST раз в секунду, история за 15 минут. Без внешних библиотек. */
class BinanceFeed {
  private hist: Record<string, { t: number; p: number }[]> = {};
  private hostIdx = 0;
  private failStreak = 0;
  private polling = false;
  okCount = 0;
  failCount = 0;

  start(): void {
    void this.poll();
    setInterval(() => void this.poll(), BINANCE_POLL_MS);
  }

  private async poll(): Promise<void> {
    if (this.polling) return;
    this.polling = true;
    const symbols = Object.values(BINANCE_SYMBOLS);
    const url = `${BINANCE_HOSTS[this.hostIdx]}/api/v3/ticker/price?symbols=${encodeURIComponent(JSON.stringify(symbols))}`;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 3000);
    try {
      const resp = await fetch(url, { signal: ctrl.signal });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const arr: any[] = await resp.json();
      const t = Date.now();
      for (const row of arr) {
        const coin = Object.keys(BINANCE_SYMBOLS).find((c) => BINANCE_SYMBOLS[c] === row.symbol);
        const p = Number(row.price);
        if (!coin || !Number.isFinite(p) || p <= 0) continue;
        const h = this.hist[coin] ?? (this.hist[coin] = []);
        h.push({ t, p });
        const cutoff = t - BINANCE_KEEP_MS;
        while (h.length > 0 && h[0].t < cutoff) h.shift();
      }
      this.okCount++;
      this.failStreak = 0;
    } catch {
      this.failCount++;
      this.failStreak++;
      if (this.failStreak >= 5) {
        this.hostIdx = (this.hostIdx + 1) % BINANCE_HOSTS.length; // пробуем другой адрес
        this.failStreak = 0;
      }
    } finally {
      clearTimeout(timer);
      this.polling = false;
    }
  }

  getLatest(coin: string): number | null {
    const h = this.hist[coin];
    if (!h || h.length === 0) return null;
    const last = h[h.length - 1];
    return Date.now() - last.t <= BINANCE_FRESH_MS ? last.p : null;
  }

  getPriceAt(coin: string, ts: number): number | null {
    const h = this.hist[coin];
    if (!h || h.length === 0) return null;
    let best: { t: number; p: number } | null = null;
    let bestD = Infinity;
    for (let i = h.length - 1; i >= 0; i--) {
      const d = Math.abs(h[i].t - ts);
      if (d < bestD) {
        best = h[i];
        bestD = d;
      }
      if (h[i].t < ts - BINANCE_MATCH_MS) break;
    }
    return best !== null && bestD <= BINANCE_MATCH_MS ? best.p : null;
  }
}
const binance = new BinanceFeed();

const BIN_LABELS = [
  "Binance против (уже за ценой открытия)",
  "Binance прошёл меньше 50% от потока",
  "50-80% от потока",
  "80-120% (примерно как поток)",
  "Binance дальше от открытия, чем поток (больше 120%)",
];
function binBucketIdx(r: number): number {
  return r < 0 ? 0 : r < 0.5 ? 1 : r < 0.8 ? 2 : r < 1.2 ? 3 : 4;
}

const AG_LABELS = ["сигнал дал только он (1 профиль)", "согласны 2 профиля", "согласны 3 и больше"];

const DIP_LABELS = ["не опускался ниже 0.95", "0.90-0.95", "0.80-0.90", "0.70-0.80", "0.50-0.70", "0.30-0.50", "ниже 0.30"];
function dipBucketIdx(minBid: number): number {
  return minBid >= 0.95 ? 0 : minBid >= 0.9 ? 1 : minBid >= 0.8 ? 2 : minBid >= 0.7 ? 3 : minBid >= 0.5 ? 4 : minBid >= 0.3 ? 5 : 6;
}
const STOP_LEVELS = [0.92, 0.9, 0.85, 0.8, 0.7, 0.6, 0.5, 0.4];
const LOSS_LOG_MAX = 60;

function fmtPct(v: number | null | undefined, d = 2): string {
  return v === null || v === undefined ? "—" : `${(v * 100).toFixed(d)}%`;
}
function fmtNum(v: number): string {
  const a = Math.abs(v);
  return a >= 100 ? v.toFixed(0) : a >= 10 ? v.toFixed(1) : v.toFixed(2);
}

const FEATURE_DEFS: { name: string; labels: string[]; bucket: (e: LabEntry) => string | null }[] = [
  {
    name: "скорость монеты за 10с (в нашу сторону)",
    labels: ["против", "почти стоит (меньше 0.02%)", "0.02-0.05%", "больше 0.05%"],
    bucket: (e) =>
      e.sp10 == null ? null : e.sp10 < 0 ? "против" : e.sp10 < 0.0002 ? "почти стоит (меньше 0.02%)" : e.sp10 < 0.0005 ? "0.02-0.05%" : "больше 0.05%",
  },
  {
    name: "скорость монеты за 30с (в нашу сторону)",
    labels: ["против", "до 0.05%", "0.05-0.15%", "больше 0.15%"],
    bucket: (e) => (e.sp30 == null ? null : e.sp30 < 0 ? "против" : e.sp30 < 0.0005 ? "до 0.05%" : e.sp30 < 0.0015 ? "0.05-0.15%" : "больше 0.15%"),
  },
  {
    name: "час суток (UTC)",
    labels: ["00-06", "06-12", "12-18", "18-24"],
    bucket: (e) => (e.hr === undefined ? null : e.hr < 6 ? "00-06" : e.hr < 12 ? "06-12" : e.hr < 18 ? "12-18" : "18-24"),
  },
  {
    name: "размер bid к размеру ask в стакане",
    labels: ["bid меньше ask", "bid в 1-3 раза больше", "bid в 3+ раз больше"],
    bucket: (e) => {
      if (e.bsz == null || e.asz == null) return null;
      if (e.asz <= 0) return "bid в 3+ раз больше";
      const r = e.bsz / e.asz;
      return r < 1 ? "bid меньше ask" : r < 3 ? "bid в 1-3 раза больше" : "bid в 3+ раз больше";
    },
  },
  {
    name: "сила движения z",
    labels: ["до 2", "2-3", "3-4", "4 и больше"],
    bucket: (e) => (e.z == null ? null : e.z < 2 ? "до 2" : e.z < 3 ? "2-3" : e.z < 4 ? "3-4" : "4 и больше"),
  },
  {
    name: "сколько других монет идут туда же (от 0.05%)",
    labels: ["0", "1", "2", "3", "4"],
    bucket: (e) => String(Math.min(e.c05, 4)),
  },
  {
    name: "пересечения цены открытия за 3 мин",
    labels: ["0", "1", "2", "3+"],
    bucket: (e) => (e.xings === undefined ? null : e.xings >= 3 ? "3+" : String(e.xings)),
  },
];

const NUM_FEATURES: { name: string; get: (e: LabEntry) => number | null }[] = [
  { name: "сила движения z", get: (e) => e.z ?? null },
  { name: "скорость монеты за 10с, %", get: (e) => (e.sp10 == null ? null : e.sp10 * 100) },
  { name: "скорость монеты за 30с, %", get: (e) => (e.sp30 == null ? null : e.sp30 * 100) },
  { name: "движение от открытия по потоку бота, %", get: (e) => (e.pm == null ? null : e.pm * 100) },
  { name: "движение от открытия по Binance, %", get: (e) => (e.bin == null ? null : e.bin * 100) },
  {
    name: "размер bid / размер ask",
    get: (e) => (e.bsz == null || e.asz == null || e.asz <= 0 ? null : Math.min(e.bsz / e.asz, 50)),
  },
  { name: "другие монеты идут туда же (0.05%)", get: (e) => e.c05 },
  { name: "пересечения цены открытия", get: (e) => e.xings ?? null },
  { name: "сколько профилей согласны", get: (e) => e.agreeN ?? null },
  { name: "секунд до закрытия", get: (e) => e.sec },
  { name: "цена входа", get: (e) => e.price },
];

/** Строки «что было бы, если не входить в корзинах с индексом меньше upTo». */
function cutLines(buckets: Stat[], cuts: { upTo: number; label: string }[]): string[] {
  const out: string[] = [];
  const tot = buckets.reduce((a, s) => ({ w: a.w + s.w, t: a.t + s.t, roi: a.roi + s.roi }), { w: 0, t: 0, roi: 0 });
  if (tot.t === 0) return out;
  const baseRoi = tot.roi / tot.t;
  for (const c of cuts) {
    let kw = 0;
    let kt = 0;
    let kr = 0;
    let cw = 0;
    let ct = 0;
    buckets.forEach((s, n) => {
      if (n >= c.upTo) {
        kw += s.w;
        kt += s.t;
        kr += s.roi;
      } else {
        cw += s.w;
        ct += s.t;
      }
    });
    if (ct === 0) {
      out.push(`  ${c.label}: ничего не отсекает`);
      continue;
    }
    const helps = kt > 0 && kr / kt > baseRoi && ct - cw > cw;
    out.push(
      `  ${c.label}: остаётся ${kw}/${kt} (${winPct(kw, kt)}), прибыль ${roiLabel(kr, kt)}; отсекли ${ct} (побед ${cw}, проигрышей ${ct - cw}) ${helps ? "✅ помогает" : "— не помогает"}`,
    );
  }
  return out;
}

// ─────────────────────────── Профили бота (моделирование) ───────────────────────────

interface BotProfile {
  id: number;
  mode: "fixed" | "adaptive" | "margin";
  priceMode: "corridor" | "touch-drop" | "none" | "retouch";
  priceLow?: number;
  priceHigh?: number;
  touchAbove?: number;
  retouchDropBelow?: number;
  requireLiveBid?: boolean; // как в боте: живой bid не ниже priceLow
  windowSec: Record<string, number>;
  param: Record<string, number>; // fixed: доля (0.0015 = 0.15%), adaptive: множитель, margin: минимальный z
}

const DEFAULT_LOW = 0.97;
const DEFAULT_HIGH = 0.98;

const BOT_PROFILES: BotProfile[] = [
  {
    id: 1,
    mode: "fixed",
    priceMode: "corridor",
    windowSec: { Bitcoin: 180, Ethereum: 60, Solana: 90, XRP: 180, Dogecoin: 90 },
    param: { Bitcoin: 0.0015, Ethereum: 0.003, Solana: 0.002, XRP: 0.003, Dogecoin: 0.0015 },
  },
  {
    id: 2,
    mode: "adaptive",
    priceMode: "corridor",
    windowSec: { Bitcoin: 120, Ethereum: 30, Solana: 30, XRP: 30, Dogecoin: 60 },
    param: { Bitcoin: 3.0, Ethereum: 0.3, Solana: 0.5, XRP: 0.3, Dogecoin: 1.0 },
  },
  {
    id: 3,
    mode: "fixed",
    priceMode: "touch-drop",
    touchAbove: 0.995,
    priceLow: 0.985,
    priceHigh: 0.993,
    windowSec: { Bitcoin: 180, Ethereum: 180, Solana: 90, XRP: 180, Dogecoin: 90 },
    param: { Bitcoin: 0.0013, Ethereum: 0.0014, Solana: 0.0015, XRP: 0.0015, Dogecoin: 0.0015 },
  },
  {
    id: 4,
    mode: "fixed",
    priceMode: "none",
    windowSec: { Bitcoin: 10, Ethereum: 10, Solana: 10, XRP: 30, Dogecoin: 60 },
    param: { Bitcoin: 0.002, Ethereum: 0.001, Solana: 0.001, XRP: 0.0014, Dogecoin: 0.003 },
  },
  {
    id: 5,
    mode: "fixed",
    priceMode: "retouch",
    priceLow: 0.97,
    priceHigh: 0.98,
    retouchDropBelow: 0.9,
    windowSec: { Bitcoin: 180, Ethereum: 180, Solana: 180, XRP: 180, Dogecoin: 120 },
    param: { Bitcoin: 0.001, Ethereum: 0.0013, Solana: 0.002, XRP: 0.001, Dogecoin: 0.001 },
  },
  {
    // Профиль 6 «Запас» — как в боте v7.7: z вместо порога, коридор 0.97-0.98,
    // вход только при живом bid не ниже 0.97.
    id: 6,
    mode: "margin",
    priceMode: "corridor",
    requireLiveBid: true,
    windowSec: { Bitcoin: 60, Ethereum: 180, Solana: 60, XRP: 180, Dogecoin: 120 },
    param: { Bitcoin: 2.0, Ethereum: 4.0, Solana: 1.5, XRP: 2.5, Dogecoin: 1.0 },
  },
  {
    // Профиль 7 «Модель» — как в боте v7.9: z от 2.0, последние 180с, цена токена 0.93-0.96,
    // вход только при живом bid не ниже 0.93.
    id: 7,
    mode: "margin",
    priceMode: "corridor",
    priceLow: 0.93,
    priceHigh: 0.96,
    requireLiveBid: true,
    windowSec: { Bitcoin: 180, Ethereum: 180, Solana: 180, XRP: 180, Dogecoin: 180 },
    param: { Bitcoin: 2.0, Ethereum: 2.0, Solana: 2.0, XRP: 2.0, Dogecoin: 2.0 },
  },
];

// ─────────────────────────── Типы и хелперы ───────────────────────────

interface Stat {
  w: number;
  t: number;
  roi: number; // сумма доходностей на $1 ставки
  px?: number; // сумма цен входа (чтобы считать среднюю цену)
}
type StatMap = Record<string, Stat>;

function bump(m: StatMap, key: string, won: boolean, roi: number, price?: number): void {
  const s = m[key] ?? (m[key] = { w: 0, t: 0, roi: 0 });
  s.t++;
  if (won) s.w++;
  s.roi += roi;
  if (price !== undefined) s.px = (s.px ?? 0) + price;
}

interface LabEntry {
  idea: string; // "margin" | "cheap" | "panic" | "impulse" | "model" | "bot1".."bot6"
  combo: string;
  coin: string;
  side: "Up" | "Down";
  ts: number;
  price: number; // цена входа = живой ask
  sec: number; // секунд до закрытия в момент входа
  c05: number; // сколько других монет идут в ту же сторону >= 0.05%
  c10: number; // то же для >= 0.10%
  xings?: number; // сколько раз монета пересекла цену открытия за последние 3 мин (только для bot-входов)
  // признаки входа (только для bot-входов, v2.4)
  z?: number | null; // сила движения
  sp10?: number | null; // движение монеты за 10с в сторону токена (доля)
  sp30?: number | null; // то же за 30с
  hr?: number; // час UTC
  bsz?: number | null; // размер лучшего bid (акции) по живому стакану
  asz?: number | null; // размер лучшего ask
  bin?: number | null; // движение Binance от открытия окна в сторону токена (доля)
  pm?: number; // движение монеты по потоку бота от открытия окна в сторону токена (доля)
  agreeN?: number; // сколько профилей бота дали сигнал в тот же момент
  minBid?: number; // минимальный bid токена после входа (до закрытия окна)
  stops?: (number | null)[]; // по каждому стоп-уровню: bid в момент первого касания или null
}

interface Feat {
  z: number | null;
  sp10: number | null;
  sp30: number | null;
  hr: number;
  bsz: number | null;
  asz: number | null;
  bin: number | null;
  pm: number;
}

interface StopAgg {
  n: number; // входов
  w: number; // из них выиграло
  stopped: number; // сколько вышибло бы стопом
  stopW: number; // из них выигрышных (стоп сработал зря)
  stopL: number; // из них проигрышных (стоп поймал)
  roiNo: number; // сумма прибыли без стопа
  roiStop: number; // сумма прибыли со стопом
}

interface PairEntry {
  pk: string; // `${окноBTC}:${движениеBTC}:${окноETH}:${движениеETH}:${индекс коридора}`
  side: "Up" | "Down";
  ts: number;
  closeTimeMs: number;
  btcSlug: string;
  ethSlug: string;
  btcPrice: number; // живой ask BTC-токена в момент входа
  ethPrice: number; // живой ask ETH-токена в момент входа
  sec: number;
}

interface LegState {
  sec: number;
  dir: number; // движение монеты в сторону токена (плюс = в нашу сторону)
  price: number; // bid, иначе ask — как в боте
  ask: number;
}

interface ArbMarketInfo {
  minSum: number;
  sec99: number | null;
  size99: number | null; // сколько акций доступно по лучшим ask в момент первой суммы <= 0.99
}

interface PendingMarket {
  closeTimeMs: number;
  coin: string;
  entries: LabEntry[];
  arb: ArbMarketInfo | null;
}

interface ArbAgg {
  total: number;
  minSumTotal: number;
  h99: number;
  h98: number;
  h97: number;
  h95: number;
  sec99Sum: number;
  size99Sum: number;
}

interface TokenInfo {
  market: CryptoUpDownMarket;
  side: "Up" | "Down";
}

interface Quote {
  bid: number | null;
  ask: number | null;
}

interface FreshBook {
  bid: number | null;
  ask: number | null;
  bidSize: number;
  askSize: number;
}

interface PooledRow {
  combo: string;
  w: number;
  t: number;
  roi: number;
  px: number;
}

interface ModelRow extends PooledRow {
  wr: number; // реальный винрейт
  avgPx: number; // средняя цена входа
  lo: number; // осторожная оценка винрейта (нижняя граница Уилсона)
  edge: number; // винрейт минус цена
  roiAvg: number; // средняя прибыль на $1
  verdict: "✅" | "⚠️" | "❌";
}

function newArbAgg(): ArbAgg {
  return { total: 0, minSumTotal: 0, h99: 0, h98: 0, h97: 0, h95: 0, sec99Sum: 0, size99Sum: 0 };
}

function buildTokenIndex(markets: CryptoUpDownMarket[]): Map<string, TokenInfo> {
  const idx = new Map<string, TokenInfo>();
  for (const m of markets) {
    idx.set(m.upTokenId, { market: m, side: "Up" });
    idx.set(m.downTokenId, { market: m, side: "Down" });
  }
  return idx;
}

function isIncludedCoin(coin: string): boolean {
  return INCLUDED_COINS.some((c) => c.toUpperCase() === coin.toUpperCase());
}

function observeWindowMs(windowMinutes: number): number {
  return (windowMinutes + 1) * 60 * 1000;
}

function pctLabel(v: number): string {
  return `${(v * 100).toFixed(2)}%`;
}

function winPct(w: number, t: number): string {
  if (t <= 0) return "—";
  if (w === t) return "100%";
  const r = (100 * w) / t;
  return r >= 99.5 ? `${r.toFixed(1)}%` : `${r.toFixed(0)}%`;
}

function roiLabel(roiSum: number, t: number): string {
  if (t <= 0) return "—";
  const r = (roiSum / t) * 100;
  return `${r >= 0 ? "+" : ""}${r.toFixed(1)}%`;
}

function signedPct(v: number): string {
  const r = v * 100;
  return `${r >= 0 ? "+" : ""}${r.toFixed(1)}%`;
}

/** Осторожная оценка винрейта: нижняя граница интервала Уилсона (95%). Чем меньше записей — тем она ниже. */
function wilsonLower(w: number, t: number): number {
  if (t <= 0) return 0;
  const z = 1.96;
  const p = w / t;
  const d = 1 + (z * z) / t;
  const c = p + (z * z) / (2 * t);
  const m = z * Math.sqrt((p * (1 - p) + (z * z) / (4 * t)) / t);
  return Math.max(0, (c - m) / d);
}

function findBand(bands: [number, number][], price: number): [number, number] | null {
  for (const b of bands) {
    if (price >= b[0] && price < b[1]) return b;
  }
  return null;
}

/**
 * Сколько раз монета пересекла цену открытия окна за последние SAW_LOOKBACK_SEC секунд
 * (но не раньше открытия окна). Замер раз в SAW_STEP_SEC секунд — очень быстрые
 * пересечения между замерами можно пропустить, поэтому число — нижняя оценка.
 */
function countCrossings(feed: CoinPriceFeed, openPrice: number, openTimeMs: number, nowMs: number, coinNow: number): number {
  const from = Math.max(openTimeMs, nowMs - SAW_LOOKBACK_SEC * 1000);
  let last = 0;
  let n = 0;
  for (let t = from; t < nowMs; t += SAW_STEP_SEC * 1000) {
    const px = feed.getPriceAt(t);
    if (px === null) continue;
    const s = Math.sign(px - openPrice);
    if (s === 0) continue;
    if (last !== 0 && s !== last) n++;
    last = s;
  }
  const sNow = Math.sign(coinNow - openPrice);
  if (sNow !== 0 && last !== 0 && sNow !== last) n++;
  return n;
}

/** Живой стакан токена через REST (то же самое, что делает бот перед покупкой). */
async function fetchBook(tokenId: string): Promise<FreshBook | null> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const resp = await fetch(`${CLOB_HOST}/book?token_id=${tokenId}`, { signal: ctrl.signal });
    if (!resp.ok) return null;
    const ob: any = await resp.json();

    let bestAsk: number | null = null;
    let askSize = 0;
    for (const o of ob.asks ?? []) {
      const p = Number(o.price);
      const s = Number(o.size);
      if (!Number.isFinite(p)) continue;
      if (bestAsk === null || p < bestAsk) {
        bestAsk = p;
        askSize = Number.isFinite(s) ? s : 0;
      } else if (p === bestAsk && Number.isFinite(s)) {
        askSize += s;
      }
    }
    let bestBid: number | null = null;
    let bidSize = 0;
    for (const o of ob.bids ?? []) {
      const p = Number(o.price);
      const s = Number(o.size);
      if (!Number.isFinite(p)) continue;
      if (bestBid === null || p > bestBid) {
        bestBid = p;
        bidSize = Number.isFinite(s) ? s : 0;
      } else if (p === bestBid && Number.isFinite(s)) {
        bidSize += s;
      }
    }
    return { bid: bestBid, ask: bestAsk, bidSize, askSize };
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

async function resolveWinner(eventSlug: string): Promise<"Up" | "Down" | null> {
  try {
    const resp = await fetch(`${GAMMA_HOST}/events/slug/${eventSlug}`);
    if (!resp.ok) return null;
    const event = await resp.json();
    const market = (event.markets ?? [])[0];
    if (!market) return null;

    // 1. Рынок должен быть официально закрыт и подтверждён
    if (market.closed !== true) return null;
    if (typeof market.umaResolutionStatus === "string" && market.umaResolutionStatus !== "resolved") return null;

    // 2. Главный источник — официальное поле winner в CLOB
    if (market.conditionId) {
      try {
        const r = await fetch(`https://clob.polymarket.com/markets/${market.conditionId}`);
        if (r.ok) {
          const m: any = await r.json();
          const win = (m.tokens ?? []).find((t: any) => t.winner === true);
          if (win) {
            const o = String(win.outcome).trim().toLowerCase();
            console.log(`[resolve] ${eventSlug}: CLOB winner = ${o}`);
            if (o === "up") return "Up";
            if (o === "down") return "Down";
          }
        }
      } catch {
        // упало — идём по запасному варианту ниже
      }
    }

    // 3. Запасной вариант: цены, но только совсем крайние (0.99 / 0.01)
    let outcomes: string[];
    let outcomePrices: string[];
    try {
      outcomes = JSON.parse(market.outcomes ?? "[]");
      outcomePrices = JSON.parse(market.outcomePrices ?? "[]");
    } catch {
      return null;
    }
    if (outcomes.length !== 2 || outcomePrices.length !== 2) return null;
    const upIdx = outcomes.findIndex((o) => /^up$/i.test(o.trim()));
    const downIdx = outcomes.findIndex((o) => /^down$/i.test(o.trim()));
    if (upIdx === -1 || downIdx === -1) return null;
    const upPrice = Number(outcomePrices[upIdx]);
    const downPrice = Number(outcomePrices[downIdx]);
    console.log(`[resolve] ${eventSlug}: по ценам Up=${upPrice} Down=${downPrice}`);
    if (upPrice >= 0.99 && downPrice <= 0.01) return "Up";
    if (downPrice >= 0.99 && upPrice <= 0.01) return "Down";
    return null;
  } catch (err) {
    console.error(`[resolve] ошибка ${eventSlug}:`, (err as Error).message);
    return null;
  }
}

// ─────────────────────────── Основной класс ───────────────────────────

class ResearchLab {
  private watcher: PriceWatcher | null = null;
  private tokenIndex = new Map<string, TokenInfo>();
  private activeMarkets = new Map<string, CryptoUpDownMarket>();
  private lastTokenIds: string[] = [];

  private quotes = new Map<string, Quote>(); // tokenId -> последние известные bid/ask
  private peaks = new Map<string, number>(); // `${slug}:${side}` -> максимальная цена токена
  private openPrices = new Map<string, number>(); // slug -> цена монеты на открытии окна
  private touchStates = new Map<string, { touchedHigh: boolean; touchedCorridor: boolean; leftAfterTouch: boolean }>();
  private firedKeys = new Set<string>();

  private verifying = new Set<string>(); // токены, по которым идёт проверка живым стаканом
  private lastVerifyAt = new Map<string, number>();
  private arbSampling = false;

  private pending = new Map<string, PendingMarket>();
  private settled = new Set<string>();

  private agg: StatMap = {}; // `${idea}|${combo}|${coin}`
  private conf: StatMap = {}; // `${idea}|c05|${n}` и `${idea}|c10|${n}`
  private agree: StatMap = {}; // `n|${k}` и `set|${1+2+4}`
  private saw: StatMap = {}; // `bot${id}|x${n}` и `all|${coin}|x${n}`
  private arbAgg: Record<string, ArbAgg> = {};

  // «Пара BTC+ETH»
  private pairAgg: StatMap = {}; // `${pk}|pair` | `${pk}|btc` | `${pk}|eth` | `${pk}|bothlost`
  private pairs: PairEntry[] = []; // входы пар, ждут резолва обоих рынков
  private pairFired = new Set<string>(); // `${pk}|${closeTimeMs}|${side}`
  private pairVerifying = new Set<string>();
  private pairLastVerify = new Map<string, number>();
  private winners = new Map<string, "Up" | "Down">(); // slug -> победитель (для расчёта пар)

  // v2.4: Binance, признаки, согласие, стопы
  private binAgg: StatMap = {}; // `${bot|all}|b${i}`
  private agAgg: StatMap = {}; // `${bot|all}|n${1..3}`
  private featAgg: StatMap = {}; // `${название признака}|${корзина}`
  private featSum: Record<string, { s: number; n: number }> = {}; // `${название}|W` / `|L`
  private lossLog: LabEntry[] = [];
  private dipAgg: StatMap = {}; // `${bot|all}|d${i}`
  private stopAgg: Record<string, StopAgg> = {}; // `${bot|all}|${индекс уровня}`

  // счётчики честности цен
  private staleSampled = 0; // сколько раз сравнили ask в памяти с живым (замеры арбитража)
  private staleShifted = 0; // из них ask в памяти отличался на STALE_DIFF и больше (или пропал)
  private verifyConfirmed = 0; // кандидат подтвердился по живому стакану
  private verifyRejected = 0; // кандидат отклонён по живому стакану

  private marketsSeenCount = 0;
  private resolvedCount = 0;
  private updateCount = 0;

  // ───── сохранение / загрузка ─────

  saveState(): void {
    try {
      const data = {
        version: STATE_VERSION,
        savedAt: Date.now(),
        agg: this.agg,
        conf: this.conf,
        agree: this.agree,
        saw: this.saw,
        pairAgg: this.pairAgg,
        binAgg: this.binAgg,
        agAgg: this.agAgg,
        featAgg: this.featAgg,
        featSum: this.featSum,
        lossLog: this.lossLog,
        dipAgg: this.dipAgg,
        stopAgg: this.stopAgg,
        pairs: this.pairs,
        winners: [...this.winners.entries()],
        arbAgg: this.arbAgg,
        pending: [...this.pending.entries()],
        settled: [...this.settled].slice(-MAX_SETTLED_REMEMBERED),
        marketsSeenCount: this.marketsSeenCount,
        resolvedCount: this.resolvedCount,
        updateCount: this.updateCount,
        staleSampled: this.staleSampled,
        staleShifted: this.staleShifted,
        verifyConfirmed: this.verifyConfirmed,
        verifyRejected: this.verifyRejected,
      };
      const tmpFile = `${STATE_FILE}.tmp`;
      fs.writeFileSync(tmpFile, JSON.stringify(data), "utf-8");
      fs.renameSync(tmpFile, STATE_FILE);
    } catch (err) {
      console.error("[saveState] ошибка сохранения:", (err as Error).message);
    }
  }

  loadState(): void {
    if (!fs.existsSync(STATE_FILE)) {
      console.log("[loadState] файл состояния не найден — начинаем с нуля.");
      return;
    }
    try {
      const data = JSON.parse(fs.readFileSync(STATE_FILE, "utf-8"));
      if (data.version !== STATE_VERSION) {
        fs.renameSync(STATE_FILE, STATE_FILE_V1);
        console.log(
          "[loadState] найден файл состояния старой версии (v1): сохранён как research-lab-state.v1.json, статистика начинается с нуля.",
        );
        return;
      }
      this.agg = data.agg ?? {};
      this.conf = data.conf ?? {};
      this.agree = data.agree ?? {};
      this.saw = data.saw ?? {};
      this.pairAgg = data.pairAgg ?? {};
      this.binAgg = data.binAgg ?? {};
      this.agAgg = data.agAgg ?? {};
      this.featAgg = data.featAgg ?? {};
      this.featSum = data.featSum ?? {};
      this.lossLog = data.lossLog ?? [];
      this.dipAgg = data.dipAgg ?? {};
      this.stopAgg = data.stopAgg ?? {};
      this.pairs = data.pairs ?? [];
      this.winners = new Map(data.winners ?? []);
      for (const e of this.pairs) this.pairFired.add(`${e.pk}|${e.closeTimeMs}|${e.side}`);
      this.arbAgg = data.arbAgg ?? {};
      this.pending = new Map(data.pending ?? []);
      this.settled = new Set(data.settled ?? []);
      this.marketsSeenCount = data.marketsSeenCount ?? 0;
      this.resolvedCount = data.resolvedCount ?? 0;
      this.updateCount = data.updateCount ?? 0;
      this.staleSampled = data.staleSampled ?? 0;
      this.staleShifted = data.staleShifted ?? 0;
      this.verifyConfirmed = data.verifyConfirmed ?? 0;
      this.verifyRejected = data.verifyRejected ?? 0;

      for (const [slug, p] of this.pending) {
        for (const e of p.entries) this.firedKeys.add(`${e.idea}|${e.combo}|${slug}|${e.side}`);
      }
      console.log(
        `[loadState] восстановлено: рынков обработано ${this.resolvedCount}, ждут резолва ${this.pending.size}.`,
      );
    } catch (err) {
      console.error("[loadState] ошибка загрузки, начинаем с нуля:", (err as Error).message);
    }
  }

  // ───── рынки ─────

  private ensurePending(market: CryptoUpDownMarket): PendingMarket | null {
    if (this.settled.has(market.eventSlug)) return null;
    let p = this.pending.get(market.eventSlug);
    if (!p) {
      if (market.closeTimeMs <= Date.now()) return null;
      p = { closeTimeMs: market.closeTimeMs, coin: market.coin, entries: [], arb: null };
      this.pending.set(market.eventSlug, p);
      this.marketsSeenCount++;
    }
    return p;
  }

  private dropPending(slug: string): void {
    const p = this.pending.get(slug);
    if (p) {
      for (const e of p.entries) this.firedKeys.delete(`${e.idea}|${e.combo}|${slug}|${e.side}`);
    }
    this.pending.delete(slug);
    this.settled.add(slug);
  }

  async refreshMarkets(): Promise<void> {
    let allMarkets: CryptoUpDownMarket[];
    try {
      allMarkets = await discoverCryptoUpDownMarkets(TIMEFRAMES_TO_DISCOVER);
    } catch (err) {
      console.error("[refresh] ошибка:", (err as Error).message);
      return;
    }

    const now = Date.now();
    const markets = allMarkets.filter(
      (m) =>
        isIncludedCoin(m.coin) &&
        m.windowMinutes === TARGET_WINDOW_MINUTES &&
        m.closeTimeMs - now <= observeWindowMs(m.windowMinutes),
    );

    this.tokenIndex = buildTokenIndex(markets);
    this.activeMarkets = new Map(markets.map((m) => [m.eventSlug, m]));
    const tokenIds = [...this.tokenIndex.keys()].sort();

    for (const m of markets) this.ensurePending(m);

    // чистим данные для рынков, которых больше нет в наблюдении
    const activeSlugs = new Set(markets.map((m) => m.eventSlug));
    for (const slug of this.openPrices.keys()) {
      if (!activeSlugs.has(slug)) this.openPrices.delete(slug);
    }
    for (const key of this.touchStates.keys()) {
      const slug = key.split(":")[1];
      if (!activeSlugs.has(slug)) this.touchStates.delete(key);
    }
    for (const key of this.peaks.keys()) {
      const slug = key.split(":")[0];
      if (!activeSlugs.has(slug)) this.peaks.delete(key);
    }
    for (const tokenId of this.quotes.keys()) {
      if (!this.tokenIndex.has(tokenId)) this.quotes.delete(tokenId);
    }
    for (const tokenId of this.lastVerifyAt.keys()) {
      if (!this.tokenIndex.has(tokenId)) this.lastVerifyAt.delete(tokenId);
    }
    for (const [k, ts] of this.pairLastVerify) {
      if (now - ts > PAIR_PRUNE_MS) this.pairLastVerify.delete(k);
    }

    console.log(
      `[refresh] наблюдаем 5-мин рынков: ${markets.length} (${tokenIds.length} токенов), ждём резолва: ${this.pending.size}`,
    );

    const sameAsLastTime =
      tokenIds.length === this.lastTokenIds.length && tokenIds.every((id, i) => id === this.lastTokenIds[i]);
    if (sameAsLastTime && this.watcher) return;

    this.lastTokenIds = tokenIds;
    this.watcher?.stop();

    if (tokenIds.length === 0) {
      this.watcher = null;
      return;
    }

    this.watcher = new PriceWatcher(tokenIds, (u) => this.onPriceUpdate(u));
    this.watcher.start();
  }

  // ───── обработка каждого апдейта цены ─────

  private onPriceUpdate(update: PriceUpdate): void {
    this.updateCount++;

    const info = this.tokenIndex.get(update.tokenId);
    if (!info) return;
    const { market, side } = info;
    const now = Date.now();
    const secToClose = (market.closeTimeMs - now) / 1000;

    // Сообщение с хотя бы одной стороной стакана — это снимок стакана: он полностью заменяет
    // старые bid/ask (пустая сторона = null). Сообщение без цен (сделка) ничего не меняет.
    const hasBid = typeof update.bestBid === "number";
    const hasAsk = typeof update.bestAsk === "number";
    const q = this.quotes.get(update.tokenId) ?? { bid: null, ask: null };
    if (hasBid || hasAsk) {
      q.bid = hasBid ? (update.bestBid as number) : null;
      q.ask = hasAsk ? (update.bestAsk as number) : null;
    }
    this.quotes.set(update.tokenId, q);

    // "цена" для условий коридора — как в боте: bestBid, иначе bestAsk
    const price = update.bestBid ?? update.bestAsk;
    if (price === null || price === undefined) return;

    // максимум цены токена за время наблюдения (для идеи «паника»)
    const peakKey = `${market.eventSlug}:${side}`;
    if (price > (this.peaks.get(peakKey) ?? 0)) this.peaks.set(peakKey, price);

    // память для профилей "touch-drop" и "retouch" — обновляется всегда, как в боте
    for (const prof of BOT_PROFILES) {
      if (prof.priceMode !== "touch-drop" && prof.priceMode !== "retouch") continue;
      const key = `${prof.id}:${market.eventSlug}:${side}`;
      let st = this.touchStates.get(key);
      if (!st) {
        st = { touchedHigh: false, touchedCorridor: false, leftAfterTouch: false };
        this.touchStates.set(key, st);
      }
      if (prof.priceMode === "touch-drop") {
        if (price >= (prof.touchAbove ?? 0.995)) st.touchedHigh = true;
      } else {
        const low = prof.priceLow ?? DEFAULT_LOW;
        const high = prof.priceHigh ?? DEFAULT_HIGH;
        if (price >= low && price <= high) st.touchedCorridor = true;
        if (st.touchedCorridor && price < (prof.retouchDropBelow ?? 0.9)) st.leftAfterTouch = true;
      }
    }

    if (secToClose < 0) return;
    const p = this.ensurePending(market);
    if (!p) return;

    // после входа отслеживаем, насколько низко опускается bid токена (для идеи «Стоп»)
    this.trackAfterEntry(side, hasBid ? (update.bestBid as number) : null, p, now);

    // «Пара BTC+ETH» — своя проверка (два рынка сразу), со своим кулдауном
    if (PAIR_COINS.includes(market.coin)) this.tryPair(market, side);

    // Быстрый предварительный отбор по данным из памяти. Если кандидата нет — выходим.
    // Если кандидат есть — НЕ записываем сразу, а проверяем живым стаканом.
    if (q.ask === null) return;
    if (this.verifying.has(update.tokenId)) return;
    if (now - (this.lastVerifyAt.get(update.tokenId) ?? 0) < VERIFY_COOLDOWN_MS) return;
    if (this.evaluate(market, side, p, q.ask, price, q.bid, false) === 0) return;

    void this.verifyAndCommit(market, side, p, update.tokenId);
  }

  // ───── v2.4: стоп, признаки, Binance, согласие ─────

  /** После входа запоминаем минимальный bid токена и первое касание каждого стоп-уровня. */
  private trackAfterEntry(side: "Up" | "Down", bid: number | null, p: PendingMarket, now: number): void {
    if (bid === null) return;
    for (const e of p.entries) {
      if (!e.idea.startsWith("bot") || e.side !== side || e.ts >= now) continue;
      if (e.minBid === undefined || bid < e.minBid) e.minBid = bid;
      if (!e.stops) e.stops = STOP_LEVELS.map(() => null);
      for (let i = 0; i < STOP_LEVELS.length; i++) {
        if (e.stops[i] === null && bid <= STOP_LEVELS[i]) e.stops[i] = bid;
      }
    }
  }

  private aggregateBotEntry(e: LabEntry, won: boolean, roi: number): void {
    if (e.agreeN === undefined) return; // вход записан до этой версии — признаков нет

    // Binance против потока бота
    if (e.bin !== null && e.bin !== undefined && e.pm !== undefined && e.pm > 0) {
      const bi = binBucketIdx(e.bin / e.pm);
      bump(this.binAgg, `${e.idea}|b${bi}`, won, roi, e.price);
      bump(this.binAgg, `all|b${bi}`, won, roi, e.price);
    }

    // согласие профилей в момент входа
    const n = Math.min(Math.max(e.agreeN, 1), 3);
    bump(this.agAgg, `${e.idea}|n${n}`, won, roi, e.price);
    bump(this.agAgg, `all|n${n}`, won, roi, e.price);

    // признаки
    for (const def of FEATURE_DEFS) {
      const b = def.bucket(e);
      if (b !== null) bump(this.featAgg, `${def.name}|${b}`, won, roi, e.price);
    }
    for (const nf of NUM_FEATURES) {
      const v = nf.get(e);
      if (v === null || !Number.isFinite(v)) continue;
      const k = `${nf.name}|${won ? "W" : "L"}`;
      const s = this.featSum[k] ?? (this.featSum[k] = { s: 0, n: 0 });
      s.s += v;
      s.n++;
    }
    if (!won) {
      this.lossLog.push(e);
      if (this.lossLog.length > LOSS_LOG_MAX) this.lossLog.shift();
    }

    // просадка и стопы (только если после входа были замеры)
    if (e.minBid !== undefined) {
      const di = dipBucketIdx(e.minBid);
      bump(this.dipAgg, `${e.idea}|d${di}`, won, roi, e.price);
      bump(this.dipAgg, `all|d${di}`, won, roi, e.price);
    }
    if (e.stops !== undefined) {
      for (let i = 0; i < STOP_LEVELS.length; i++) {
        const fill = e.stops[i];
        const stopped = fill !== null && fill !== undefined;
        const roiStop = stopped ? ((fill as number) - e.price) / e.price : roi;
        for (const key of [`${e.idea}|${i}`, `all|${i}`]) {
          const a = this.stopAgg[key] ?? (this.stopAgg[key] = { n: 0, w: 0, stopped: 0, stopW: 0, stopL: 0, roiNo: 0, roiStop: 0 });
          a.n++;
          if (won) a.w++;
          a.roiNo += roi;
          a.roiStop += roiStop;
          if (stopped) {
            a.stopped++;
            if (won) a.stopW++;
            else a.stopL++;
          }
        }
      }
    }
  }

  buildBinanceReport(): string {
    const get = (key: string): Stat => this.binAgg[key] ?? { w: 0, t: 0, roi: 0 };
    const lines: string[] = [];
    lines.push("<b>📊 Второе мнение: Binance</b>");
    lines.push(
      "К каждому входу профилей бота пишем, как далеко от цены открытия окна монета на Binance и как далеко она по потоку цен, по которому торгует бот. Идея: если Binance уже вернулся к цене открытия, а поток ещё нет, разворот начался раньше, и вход опасный.",
    );
    lines.push(`Опрос Binance: удачно ${binance.okCount}, ошибок ${binance.failCount}. Данные копятся с момента запуска этой версии.`);
    lines.push("");
    const sections: [string, string][] = [["all", "Все профили вместе"], ...BOT_PROFILES.map((p) => [`bot${p.id}`, `Профиль ${p.id}`] as [string, string])];
    for (const [key, title] of sections) {
      const buckets = BIN_LABELS.map((_, i) => get(`${key}|b${i}`));
      const tot = buckets.reduce((a, s) => ({ w: a.w + s.w, t: a.t + s.t, roi: a.roi + s.roi }), { w: 0, t: 0, roi: 0 });
      if (tot.t === 0) {
        if (key === "all") {
          lines.push("<b>── Все профили вместе ──</b>");
          lines.push("  пока нет данных");
          lines.push("");
        }
        continue;
      }
      lines.push(`<b>── ${title} (входов ${tot.t}, ${winPct(tot.w, tot.t)}) ──</b>`);
      buckets.forEach((s, i) => {
        if (s.t === 0) return;
        lines.push(`  ${BIN_LABELS[i]}: ${s.w}/${s.t} (${winPct(s.w, s.t)}), прибыль ${roiLabel(s.roi, s.t)}${s.t < 10 ? " (мало данных)" : ""}`);
      });
      lines.push(
        ...cutLines(buckets, [
          { upTo: 1, label: "не входить, если Binance против" },
          { upTo: 2, label: "не входить, если Binance прошёл меньше 50% от потока" },
          { upTo: 3, label: "не входить, если Binance прошёл меньше 80% от потока" },
        ]),
      );
      lines.push("");
    }
    lines.push("Прибыль — средний результат на $1 ставки (без комиссий). Binance — цена в USDT, поток бота — цена в USD, для процентов от открытия разницы почти нет.");
    return lines.join("\n");
  }

  buildAgreeReport(): string {
    const get = (key: string): Stat => this.agAgg[key] ?? { w: 0, t: 0, roi: 0 };
    const lines: string[] = [];
    lines.push("<b>📊 Согласие профилей в момент входа</b>");
    lines.push(
      "Сколько профилей бота дали сигнал одновременно в тот момент, когда сработал этот вход. Идея: если сигнал даёт сразу несколько профилей, вход надёжнее.",
    );
    lines.push("");
    const sections: [string, string][] = [["all", "Все профили вместе"], ...BOT_PROFILES.map((p) => [`bot${p.id}`, `Профиль ${p.id}`] as [string, string])];
    for (const [key, title] of sections) {
      const buckets = [1, 2, 3].map((n) => get(`${key}|n${n}`));
      const tot = buckets.reduce((a, s) => ({ w: a.w + s.w, t: a.t + s.t, roi: a.roi + s.roi }), { w: 0, t: 0, roi: 0 });
      if (tot.t === 0) {
        if (key === "all") {
          lines.push("<b>── Все профили вместе ──</b>");
          lines.push("  пока нет данных");
          lines.push("");
        }
        continue;
      }
      lines.push(`<b>── ${title} (входов ${tot.t}, ${winPct(tot.w, tot.t)}) ──</b>`);
      buckets.forEach((s, i) => {
        if (s.t === 0) return;
        lines.push(`  ${AG_LABELS[i]}: ${s.w}/${s.t} (${winPct(s.w, s.t)}), прибыль ${roiLabel(s.roi, s.t)}${s.t < 10 ? " (мало данных)" : ""}`);
      });
      lines.push(
        ...cutLines(buckets, [
          { upTo: 1, label: "брать только если согласны 2+ профиля" },
          { upTo: 2, label: "брать только если согласны 3+ профиля" },
        ]),
      );
      lines.push("");
    }
    lines.push("Прибыль — средний результат на $1 ставки (без комиссий).");
    return lines.join("\n");
  }

  buildLossReport(): string {
    const lines: string[] = [];
    let totW = 0;
    let totT = 0;
    for (let n = 1; n <= 3; n++) {
      const s = this.agAgg[`all|n${n}`];
      if (s) {
        totW += s.w;
        totT += s.t;
      }
    }
    lines.push("<b>📊 Вскрытие лосей</b>");
    lines.push(
      `К каждому входу профилей бота записываем признаки момента входа и сравниваем проигравшие входы с выигравшими. Входов с признаками: ${totT}, из них проиграло ${totT - totW}. Данные копятся с момента запуска этой версии.`,
    );
    lines.push("");

    lines.push("<b>── Чем проигравшие отличаются от выигравших (средние значения) ──</b>");
    if (totT === 0) {
      lines.push("  пока нет данных");
    } else {
      for (const nf of NUM_FEATURES) {
        const w = this.featSum[`${nf.name}|W`];
        const l = this.featSum[`${nf.name}|L`];
        if (!w && !l) continue;
        const wTxt = w && w.n > 0 ? `${fmtNum(w.s / w.n)} (${w.n})` : "—";
        const lTxt = l && l.n > 0 ? `${fmtNum(l.s / l.n)} (${l.n})` : "—";
        lines.push(`  ${nf.name}: выигравшие ${wTxt} | проигравшие ${lTxt}`);
      }
      lines.push(`(в скобках — сколько входов в среднем; при ${totT - totW < 10 ? "малом числе лосей" : "числе лосей"} различия могут быть случайными)`);
    }
    lines.push("");

    lines.push("<b>── Винрейт по признакам ──</b>");
    for (const def of FEATURE_DEFS) {
      const parts: string[] = [];
      for (const label of def.labels) {
        const s = this.featAgg[`${def.name}|${label}`];
        if (!s || s.t === 0) continue;
        parts.push(`${label}: ${s.w}/${s.t} ${winPct(s.w, s.t)}`);
      }
      lines.push(`  ${def.name}`);
      lines.push(parts.length > 0 ? `     ${parts.join(" | ")}` : "     пока нет данных");
    }
    lines.push("");

    lines.push("<b>── Последние проигрыши (до 15) ──</b>");
    if (this.lossLog.length === 0) {
      lines.push("  пока нет проигрышей с признаками");
    } else {
      for (const e of [...this.lossLog].slice(-15).reverse()) {
        const t = new Date(e.ts).toISOString().slice(11, 16);
        lines.push(
          `  ${t} UTC ${COIN_SHORT[e.coin] ?? e.coin} ${e.side}, профиль ${e.idea.slice(3)}, цена ${e.price.toFixed(2)}, за ${e.sec}с | z ${e.z != null ? e.z.toFixed(1) : "—"} | скорость 10с ${fmtPct(e.sp10)}, 30с ${fmtPct(e.sp30)} | bid/ask ${e.bsz != null ? Math.round(e.bsz) : "—"}/${e.asz != null ? Math.round(e.asz) : "—"} | Binance ${fmtPct(e.bin)} (поток ${fmtPct(e.pm)}) | согласны профилей ${e.agreeN ?? "—"} | пересечений ${e.xings ?? "—"} | мин. цена после входа ${e.minBid != null ? e.minBid.toFixed(2) : "—"}`,
        );
      }
    }
    return lines.join("\n");
  }

  buildStopReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Стоп-лосс: как глубоко проседает цена после входа</b>");
    lines.push(
      "Для каждого входа профилей бота пишем минимальную цену токена (bid) после входа до закрытия окна. Потом считаем, что было бы, если бы при падении цены до уровня мы продавали: сколько выигрышных входов стоп выбил бы зря, сколько проигрышных поймал, и что с прибылью.",
    );
    lines.push(
      "Продажа считается по первой замеренной цене ниже уровня (без комиссий и проскальзывания, в жизни чуть хуже). Если цена обвалилась уже после закрытия окна или одним скачком, стоп не успевает: такие проигрыши остаются «не пойманными».",
    );
    lines.push("");

    const get = (key: string): Stat => this.dipAgg[key] ?? { w: 0, t: 0, roi: 0 };
    lines.push("<b>── Как глубоко проседали входы (все профили вместе) ──</b>");
    let anyDip = false;
    DIP_LABELS.forEach((label, i) => {
      const s = get(`all|d${i}`);
      if (s.t === 0) return;
      anyDip = true;
      lines.push(`  мин. цена после входа ${label}: выиграло ${s.w}, проиграло ${s.t - s.w}`);
    });
    if (!anyDip) lines.push("  пока нет данных");
    lines.push("(строки с выигравшими внизу таблицы — это шум: цена проседала, но вход всё равно выигрывал; стоп там выбил бы зря)");
    lines.push("");

    const stopLines = (key: string): string[] => {
      const out: string[] = [];
      STOP_LEVELS.forEach((lvl, i) => {
        const a = this.stopAgg[`${key}|${i}`];
        if (!a || a.n === 0) return;
        const better = a.roiStop > a.roiNo + 1e-9 ? "✅" : a.roiStop < a.roiNo - 1e-9 ? "❌" : "—";
        out.push(
          `  стоп ${lvl.toFixed(2)}: сработал ${a.stopped} из ${a.n} (зря на выигрышных ${a.stopW}, поймал проигрышных ${a.stopL} из ${a.n - a.w}); прибыль без стопа ${roiLabel(a.roiNo, a.n)}, со стопом ${roiLabel(a.roiStop, a.n)} ${better}`,
        );
      });
      return out;
    };

    lines.push("<b>── Стоп-уровни (все профили вместе) ──</b>");
    const allLines = stopLines("all");
    lines.push(...(allLines.length > 0 ? allLines : ["  пока нет данных"]));
    lines.push("");

    for (const prof of BOT_PROFILES) {
      const a0 = this.stopAgg[`bot${prof.id}|0`];
      if (!a0 || a0.n < 20) continue;
      lines.push(`<b>── Профиль ${prof.id} (входов ${a0.n}, проиграло ${a0.n - a0.w}) ──</b>`);
      lines.push(...stopLines(`bot${prof.id}`));
      lines.push("");
    }
    lines.push("Прибыль — средний результат на $1 ставки. Бот пока не умеет продавать до резолва: это только исследование.");
    return lines.join("\n");
  }

  // ───── «Пара BTC+ETH» ─────

  /** Состояние одной ноги пары при заданных ask/bid токена. null — данных не хватает. */
  private legState(m: CryptoUpDownMarket, side: "Up" | "Down", ask: number | null, bid: number | null): LegState | null {
    const feed = PRICE_FEEDS[m.coin];
    if (!feed) return null;
    const sec = (m.closeTimeMs - Date.now()) / 1000;
    if (sec < 0) return null;
    const openTimeMs = m.closeTimeMs - m.windowMinutes * 60 * 1000;
    let op = this.openPrices.get(m.eventSlug);
    if (op === undefined) {
      const v = feed.getPriceAt(openTimeMs);
      if (v === null) return null;
      op = v;
      this.openPrices.set(m.eventSlug, op);
    }
    const coinNow = feed.getLatestPrice();
    if (coinNow === null || op === 0) return null;
    const pct = (coinNow - op) / op;
    const price = bid ?? ask;
    if (ask === null || price === null || ask <= 0 || ask > 0.999) return null;
    return { sec, dir: side === "Up" ? pct : -pct, price, ask };
  }

  /** Какие комбинации пары выполняются прямо сейчас (ключи pk). */
  private pairHolds(lb: LegState, le: LegState): string[] {
    const out: string[] = [];
    const wsB = PAIR_WINDOWS.filter((w) => lb.sec <= w);
    const wsE = PAIR_WINDOWS.filter((w) => le.sec <= w);
    if (wsB.length === 0 || wsE.length === 0) return out;
    const msB = PAIR_MOVES.filter((m) => lb.dir >= m);
    const msE = PAIR_MOVES.filter((m) => le.dir >= m);
    if (msB.length === 0 || msE.length === 0) return out;
    for (let bi = 0; bi < PAIR_BANDS.length; bi++) {
      const [lo, hi] = PAIR_BANDS[bi];
      const okB = lb.price >= lo && lb.price <= hi && lb.ask <= hi;
      const okE = le.price >= lo && le.price <= hi && le.ask <= hi;
      if (!okB || !okE) continue;
      for (const wB of wsB)
        for (const mB of msB)
          for (const wE of wsE)
            for (const mE of msE) out.push(`${wB}:${mB}:${wE}:${mE}:${bi}`);
    }
    return out;
  }

  private tryPair(market: CryptoUpDownMarket, side: "Up" | "Down"): void {
    const otherCoin = market.coin === PAIR_COINS[0] ? PAIR_COINS[1] : PAIR_COINS[0];
    let partner: CryptoUpDownMarket | undefined;
    for (const m of this.activeMarkets.values()) {
      if (m.coin === otherCoin && m.closeTimeMs === market.closeTimeMs) {
        partner = m;
        break;
      }
    }
    if (!partner) return;
    const btcM = market.coin === PAIR_COINS[0] ? market : partner;
    const ethM = market.coin === PAIR_COINS[0] ? partner : market;
    if (!this.pending.has(btcM.eventSlug) || !this.pending.has(ethM.eventSlug)) return;

    const key = `${market.closeTimeMs}:${side}`;
    if (this.pairVerifying.has(key)) return;
    if (Date.now() - (this.pairLastVerify.get(key) ?? 0) < VERIFY_COOLDOWN_MS) return;

    const tB = side === "Up" ? btcM.upTokenId : btcM.downTokenId;
    const tE = side === "Up" ? ethM.upTokenId : ethM.downTokenId;
    const qB = this.quotes.get(tB);
    const qE = this.quotes.get(tE);
    if (!qB || !qE) return;
    const lb = this.legState(btcM, side, qB.ask, qB.bid);
    const le = this.legState(ethM, side, qE.ask, qE.bid);
    if (!lb || !le) return;

    const fresh = this.pairHolds(lb, le).some((pk) => !this.pairFired.has(`${pk}|${btcM.closeTimeMs}|${side}`));
    if (!fresh) return;

    void this.verifyAndCommitPair(btcM, ethM, side, key);
  }

  /** Берёт живые стаканы обоих токенов и, если пара по живым ценам выполняется, записывает входы. */
  private async verifyAndCommitPair(
    btcM: CryptoUpDownMarket,
    ethM: CryptoUpDownMarket,
    side: "Up" | "Down",
    key: string,
  ): Promise<void> {
    this.pairVerifying.add(key);
    this.pairLastVerify.set(key, Date.now());
    try {
      const tB = side === "Up" ? btcM.upTokenId : btcM.downTokenId;
      const tE = side === "Up" ? ethM.upTokenId : ethM.downTokenId;
      const [bB, bE] = await Promise.all([fetchBook(tB), fetchBook(tE)]);
      if (!bB || !bE) return;
      this.quotes.set(tB, { bid: bB.bid, ask: bB.ask });
      this.quotes.set(tE, { bid: bE.bid, ask: bE.ask });
      if (!this.pending.has(btcM.eventSlug) || !this.pending.has(ethM.eventSlug)) return;

      const lb = this.legState(btcM, side, bB.ask, bB.bid);
      const le = this.legState(ethM, side, bE.ask, bE.bid);
      if (!lb || !le) return;

      const now = Date.now();
      for (const pk of this.pairHolds(lb, le)) {
        const fk = `${pk}|${btcM.closeTimeMs}|${side}`;
        if (this.pairFired.has(fk)) continue;
        this.pairFired.add(fk);
        this.pairs.push({
          pk,
          side,
          ts: now,
          closeTimeMs: btcM.closeTimeMs,
          btcSlug: btcM.eventSlug,
          ethSlug: ethM.eventSlug,
          btcPrice: lb.ask,
          ethPrice: le.ask,
          sec: Math.round(lb.sec),
        });
      }
    } catch (err) {
      console.error("[pair] ошибка:", (err as Error).message);
    } finally {
      this.pairVerifying.delete(key);
    }
  }

  /** Считает пары, у которых резолв пришёл по обоим рынкам; слишком старые без резолва выбрасывает. */
  private settlePairs(): void {
    const now = Date.now();
    const seenSituations = new Set<string>();
    for (let i = this.pairs.length - 1; i >= 0; i--) {
      const e = this.pairs[i];
      const wb = this.winners.get(e.btcSlug);
      const we = this.winners.get(e.ethSlug);
      if (wb && we) {
        const wonB = wb === e.side;
        const wonE = we === e.side;
        const roiB = wonB ? (1 - e.btcPrice) / e.btcPrice : -1;
        const roiE = wonE ? (1 - e.ethPrice) / e.ethPrice : -1;
        bump(this.pairAgg, `${e.pk}|pair`, wonB && wonE, (roiB + roiE) / 2, (e.btcPrice + e.ethPrice) / 2);
        bump(this.pairAgg, `${e.pk}|btc`, wonB, roiB, e.btcPrice);
        bump(this.pairAgg, `${e.pk}|eth`, wonE, roiE, e.ethPrice);
        bump(this.pairAgg, `${e.pk}|bothlost`, !wonB && !wonE, 0);
        // настоящая ситуация (одна на момент закрытия и сторону), без дублей по комбинациям
        const sk = `${e.closeTimeMs}:${e.side}`;
        if (!seenSituations.has(sk)) {
          seenSituations.add(sk);
          bump(this.pairAgg, "_sit|pair", wonB && wonE, 0);
          bump(this.pairAgg, "_sit|btc", wonB, 0);
          bump(this.pairAgg, "_sit|eth", wonE, 0);
        }
        this.pairs.splice(i, 1);
        this.pairFired.delete(`${e.pk}|${e.closeTimeMs}|${e.side}`);
      } else if (now - e.closeTimeMs > RESOLVE_GIVE_UP_MS + PAIR_PRUNE_MS) {
        this.pairs.splice(i, 1);
        this.pairFired.delete(`${e.pk}|${e.closeTimeMs}|${e.side}`);
      }
    }
  }

  /** Запрашивает живой стакан токена и, если условия выполняются по живым ценам, записывает вход. */
  private async verifyAndCommit(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    p: PendingMarket,
    tokenId: string,
  ): Promise<void> {
    this.verifying.add(tokenId);
    this.lastVerifyAt.set(tokenId, Date.now());
    try {
      const book = await fetchBook(tokenId);
      if (!book) return; // сеть/ошибка — просто пропускаем, попробуем позже
      this.quotes.set(tokenId, { bid: book.bid, ask: book.ask });

      if (this.pending.get(market.eventSlug) !== p) return; // рынок уже закрыт и обработан

      const price = book.bid ?? book.ask;
      if (book.ask === null || price === null) {
        this.verifyRejected++;
        return;
      }
      const recorded = this.evaluate(market, side, p, book.ask, price, book.bid, true, book);
      if (recorded > 0) this.verifyConfirmed++;
      else this.verifyRejected++;
    } catch (err) {
      console.error("[verify] ошибка:", (err as Error).message);
    } finally {
      this.verifying.delete(tokenId);
    }
  }

  /**
   * Проверяет условия всех идей для данной стороны рынка при заданных ask/price/bid.
   * commit=false — только считает, сколько новых входов записалось бы.
   * commit=true  — записывает входы. Возвращает число (новых) входов.
   */
  private evaluate(
    market: CryptoUpDownMarket,
    side: "Up" | "Down",
    p: PendingMarket,
    ask: number,
    price: number,
    bid: number | null,
    commit: boolean,
    book: FreshBook | null = null,
  ): number {
    const now = Date.now();
    const secToClose = (market.closeTimeMs - now) / 1000;
    if (secToClose < 0) return 0;
    if (ask <= 0 || ask > 0.999) return 0;

    const feed = PRICE_FEEDS[market.coin];
    if (!feed) return 0;

    const openTimeMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
    let openPrice = this.openPrices.get(market.eventSlug);
    if (openPrice === undefined) {
      const op = feed.getPriceAt(openTimeMs);
      if (op === null) return 0;
      openPrice = op;
      this.openPrices.set(market.eventSlug, openPrice);
    }
    const openP: number = openPrice;
    const coinNow = feed.getLatestPrice();
    if (coinNow === null) return 0;
    const pctMove = (coinNow - openP) / openP;
    const dirMove = side === "Up" ? pctMove : -pctMove; // плюс = монета идёт в сторону токена
    const peak = this.peaks.get(`${market.eventSlug}:${side}`) ?? 0;

    // ленивые вычисления
    let volCache: number | null | undefined;
    const getVol = (): number | null => {
      if (volCache === undefined) volCache = volTracker.getRecentVolatility(market.coin, now);
      return volCache ?? null;
    };
    // z = запас: движение монеты в сторону токена ÷ (типичное 5-мин движение × √(осталось/300))
    const getZ = (): number | null => {
      if (dirMove <= 0) return null;
      const v = getVol();
      if (v === null || v <= 1e-6) return null;
      return dirMove / (v * Math.sqrt(Math.max(secToClose, 1) / 300));
    };
    let confCache: { c05: number; c10: number } | undefined;
    const getConf = (): { c05: number; c10: number } => {
      if (confCache === undefined) {
        let c05 = 0;
        let c10 = 0;
        for (const c of INCLUDED_COINS) {
          if (c === market.coin) continue;
          const f = PRICE_FEEDS[c];
          const o = f.getPriceAt(openTimeMs);
          const n = f.getLatestPrice();
          if (o === null || n === null || o === 0) continue;
          const mv = side === "Up" ? (n - o) / o : (o - n) / o;
          if (mv >= CONFIRM_LEVEL_1) c05++;
          if (mv >= CONFIRM_LEVEL_2) c10++;
        }
        confCache = { c05, c10 };
      }
      return confCache;
    };
    // пересечения цены открытия — считаем только когда реально записываем вход профиля бота
    let xingsCache: number | undefined;
    const getXings = (): number => {
      if (xingsCache === undefined) xingsCache = countCrossings(feed, openP, openTimeMs, now, coinNow);
      return xingsCache;
    };

    // признаки входа (считаются только когда реально записываем вход профиля бота)
    let featCache: Feat | undefined;
    const getFeat = (): Feat => {
      if (featCache === undefined) {
        const sp = (lb: number): number | null => {
          const p0 = feed.getPriceAt(now - lb * 1000);
          if (p0 === null || p0 === 0) return null;
          const m = (coinNow - p0) / p0;
          return side === "Up" ? m : -m;
        };
        const bo = binance.getPriceAt(market.coin, openTimeMs);
        const bn = binance.getLatest(market.coin);
        let bin: number | null = null;
        if (bo !== null && bn !== null && bo !== 0) {
          const m = (bn - bo) / bo;
          bin = side === "Up" ? m : -m;
        }
        featCache = {
          z: getZ(),
          sp10: sp(10),
          sp30: sp(30),
          hr: new Date(now).getUTCHours(),
          bsz: book ? book.bidSize : null,
          asz: book ? book.askSize : null,
          bin,
          pm: dirMove,
        };
      }
      return featCache;
    };
    let agreeNow = 0; // сколько профилей бота выполняются прямо сейчас (заполняется ниже)

    let count = 0;
    const emit = (idea: string, combo: string): void => {
      const key = `${idea}|${combo}|${market.eventSlug}|${side}`;
      if (this.firedKeys.has(key)) return;
      count++;
      if (!commit) return;
      this.firedKeys.add(key);
      const c = getConf();
      p.entries.push({
        idea,
        combo,
        coin: market.coin,
        side,
        ts: now,
        price: ask,
        sec: Math.round(secToClose),
        c05: c.c05,
        c10: c.c10,
        xings: idea.startsWith("bot") ? getXings() : undefined,
        ...(idea.startsWith("bot") ? { ...getFeat(), agreeN: agreeNow } : {}),
      });
    };

    // ── 1. «Запас» ──
    if (price >= MARGIN_PRICE_LOW && price <= MARGIN_PRICE_HIGH && ask <= MARGIN_PRICE_HIGH && dirMove > 0) {
      const vol = getVol();
      if (vol !== null && vol > 1e-6) {
        const z = dirMove / (vol * Math.sqrt(Math.max(secToClose, 1) / 300));
        for (const w of MARGIN_WINDOWS) {
          if (secToClose > w) continue;
          for (const zl of Z_LEVELS) {
            if (z < zl) continue;
            emit("margin", `z≥${zl.toFixed(1)} / ${w}с`);
          }
        }
      }
    }

    // ── 2. «Недооценка» ──
    if (dirMove > 0) {
      const band = findBand(CHEAP_BANDS, ask);
      if (band) {
        for (const w of CHEAP_WINDOWS) {
          if (secToClose > w) continue;
          for (const thr of CHEAP_THRESHOLDS) {
            if (dirMove < thr) continue;
            emit("cheap", `${pctLabel(thr)} / ${w}с / ${band[0].toFixed(2)}-${band[1].toFixed(2)}`);
          }
        }
      }
    }

    // ── 3. «Паника без разворота» ──
    if (dirMove > 0 && peak >= PANIC_PEAK) {
      const band = findBand(PANIC_BANDS, ask);
      if (band) {
        for (const w of PANIC_WINDOWS) {
          if (secToClose > w) continue;
          for (const thr of PANIC_THRESHOLDS) {
            if (dirMove < thr) continue;
            emit("panic", `${pctLabel(thr)} / ${w}с / ${band[0].toFixed(2)}-${band[1].toFixed(2)}`);
          }
        }
      }
    }

    // ── 4. «Импульс последних секунд» ──
    if (secToClose <= 180 && dirMove > 0 && ask >= IMPULSE_ASK_MIN && ask <= IMPULSE_ASK_MAX) {
      for (const lb of IMPULSE_LOOKBACKS_SEC) {
        const p0 = feed.getPriceAt(now - lb * 1000);
        if (p0 === null || p0 === 0) continue;
        const imp = (coinNow - p0) / p0;
        const dirImp = side === "Up" ? imp : -imp;
        if (dirImp <= 0) continue;
        for (const w of IMPULSE_WINDOWS) {
          if (secToClose > w) continue;
          for (const thr of IMPULSE_THRESHOLDS) {
            if (dirImp < thr) continue;
            emit("impulse", `${pctLabel(thr)} за ${lb}с / ${w}с`);
          }
        }
      }
    }

    // ── 8. «Модель цены»: сила (z) × секунды × цена. Одна запись на комбинацию корзин. ──
    if (secToClose <= MODEL_MAX_SEC && ask >= MODEL_ASK_MIN && ask < MODEL_ASK_MAX) {
      const z = getZ();
      if (z !== null) {
        const zb = findBand(MODEL_Z_BANDS, z);
        const sb = findBand(MODEL_SEC_BANDS, secToClose);
        const pb = findBand(MODEL_PRICE_BANDS, ask);
        if (zb && sb && pb) emit("model", `${zLabel(zb)} / ${secLabel(sb)} / ${priceLabel(pb)}`);
      }
    }

    // ── 10. «Соло» для пары: та же монета, тот же триггер, но без условия на вторую монету ──
    if (PAIR_COINS.includes(market.coin) && dirMove > 0) {
      for (let bi = 0; bi < PAIR_BANDS.length; bi++) {
        const [lo, hi] = PAIR_BANDS[bi];
        if (!(price >= lo && price <= hi && ask <= hi)) continue;
        for (const w of PAIR_WINDOWS) {
          if (secToClose > w) continue;
          for (const mv of PAIR_MOVES) {
            if (dirMove < mv) continue;
            emit("psolo", `${w}:${mv}:${bi}`);
          }
        }
      }
    }

    // ── 7. Моделирование профилей бота (для «согласия профилей», «пилы» и подтверждения монетами) ──
    const holding: number[] = [];
    for (const prof of BOT_PROFILES) {
      const win = prof.windowSec[market.coin];
      if (win === undefined || secToClose > win) continue;

      const low = prof.priceLow ?? DEFAULT_LOW;
      const high = prof.priceHigh ?? DEFAULT_HIGH;
      if (ask > high) continue; // потолок покупки — как maxBuyPrice в боте

      let priceOk: boolean;
      if (prof.priceMode === "none") {
        priceOk = true;
      } else if (prof.priceMode === "touch-drop") {
        const st = this.touchStates.get(`${prof.id}:${market.eventSlug}:${side}`);
        priceOk = (st?.touchedHigh ?? false) && price >= low && price <= high;
      } else if (prof.priceMode === "retouch") {
        const st = this.touchStates.get(`${prof.id}:${market.eventSlug}:${side}`);
        priceOk = (st?.touchedCorridor ?? false) && (st?.leftAfterTouch ?? false) && price >= low && price <= high;
      } else {
        priceOk = price >= low && price <= high;
      }
      if (!priceOk) continue;

      const pv = prof.param[market.coin];
      if (pv === undefined) continue;

      // «Запас» (профиль 6): вместо порога движения — минимальный z, плюс проверка живого bid
      if (prof.mode === "margin") {
        const z = getZ();
        if (z === null || z < pv) continue;
        if (prof.requireLiveBid && (bid === null || bid < low - 1e-9)) continue;
        holding.push(prof.id);
        continue;
      }

      let thr: number;
      if (prof.mode === "adaptive") {
        const v = getVol();
        if (v === null) continue;
        thr = pv * v;
      } else {
        thr = pv;
      }

      const passes = side === "Up" ? pctMove >= thr : pctMove <= -thr;
      if (!passes) continue;

      holding.push(prof.id);
    }
    agreeNow = holding.length;
    for (const id of holding) emit(`bot${id}`, "профиль");

    return count;
  }

  // ───── арбитраж: замер живыми стаканами ─────

  async sampleArb(): Promise<void> {
    if (this.arbSampling) return;
    this.arbSampling = true;
    try {
      const list = [...this.activeMarkets.values()].filter((m) => m.closeTimeMs - Date.now() > 0);
      await Promise.all(
        list.map(async (m) => {
          const p = this.ensurePending(m);
          if (!p) return;
          const [up, down] = await Promise.all([fetchBook(m.upTokenId), fetchBook(m.downTokenId)]);
          if (!up || !down) return;

          // сравниваем ask в памяти с живым — это замер "насколько цены в памяти устаревают"
          for (const [tok, b] of [
            [m.upTokenId, up],
            [m.downTokenId, down],
          ] as const) {
            const old = this.quotes.get(tok);
            if (old && old.ask !== null) {
              this.staleSampled++;
              if (b.ask === null || Math.abs(b.ask - old.ask) >= STALE_DIFF) this.staleShifted++;
            }
            this.quotes.set(tok, { bid: b.bid, ask: b.ask });
          }

          if (up.ask === null || down.ask === null) return;
          const sum = up.ask + down.ask;
          const sec = (m.closeTimeMs - Date.now()) / 1000;
          if (!p.arb) p.arb = { minSum: sum, sec99: null, size99: null };
          if (sum < p.arb.minSum) p.arb.minSum = sum;
          if (sum <= 0.99 + 1e-9 && p.arb.sec99 === null) {
            p.arb.sec99 = sec;
            p.arb.size99 = Math.min(up.askSize, down.askSize);
          }
        }),
      );
    } catch (err) {
      console.error("[arb] ошибка замера:", (err as Error).message);
    } finally {
      this.arbSampling = false;
    }
  }

  // ───── резолв ─────

  private settle(slug: string, winner: "Up" | "Down"): void {
    const p = this.pending.get(slug);
    if (!p) return;

    this.winners.set(slug, winner);
    if (this.winners.size > 400) {
      const oldest = this.winners.keys().next().value;
      if (oldest !== undefined) this.winners.delete(oldest);
    }

    // v2.4: признаки, Binance, согласие профилей, просадка и стопы — по каждому входу профилей бота
    for (const e of p.entries) {
      if (!e.idea.startsWith("bot")) continue;
      const wonE = e.side === winner;
      this.aggregateBotEntry(e, wonE, wonE ? (1 - e.price) / e.price : -1);
    }

    const earliest = new Map<string, LabEntry>(); // `${idea}|${side}` -> самый ранний вход
    for (const e of p.entries) {
      const won = e.side === winner;
      const roi = won ? (1 - e.price) / e.price : -1;
      bump(this.agg, `${e.idea}|${e.combo}|${e.coin}`, won, roi, e.price);

      const k = `${e.idea}|${e.side}`;
      const ex = earliest.get(k);
      if (!ex || e.ts < ex.ts) earliest.set(k, e);
    }

    // подтверждение другими монетами — одна запись на (идея, сторона), самый ранний вход
    for (const e of earliest.values()) {
      const won = e.side === winner;
      const roi = won ? (1 - e.price) / e.price : -1;
      bump(this.conf, `${e.idea}|c05|${e.c05}`, won, roi);
      bump(this.conf, `${e.idea}|c10|${e.c10}`, won, roi);
    }

    // «пила» — по каждому профилю бота, самый ранний вход, со своим числом пересечений
    for (const e of earliest.values()) {
      if (!e.idea.startsWith("bot") || e.xings === undefined) continue;
      const won = e.side === winner;
      const roi = won ? (1 - e.price) / e.price : -1;
      const n = Math.min(e.xings, 3);
      bump(this.saw, `${e.idea}|x${n}`, won, roi, e.price);
      bump(this.saw, `all|${e.coin}|x${n}`, won, roi, e.price);
    }

    // согласие профилей бота — по каждой стороне рынка
    const botBySide = new Map<string, LabEntry[]>();
    for (const e of earliest.values()) {
      if (!e.idea.startsWith("bot")) continue;
      const list = botBySide.get(e.side) ?? [];
      list.push(e);
      botBySide.set(e.side, list);
    }
    for (const [side, list] of botBySide) {
      const ids = list.map((e) => e.idea.slice(3)).sort();
      const first = list.reduce((a, b) => (a.ts <= b.ts ? a : b));
      const won = side === winner;
      const roi = won ? (1 - first.price) / first.price : -1;
      bump(this.agree, `n|${ids.length}`, won, roi);
      bump(this.agree, `set|${ids.join("+")}`, won, roi);
    }

    // арбитраж
    if (p.arb) {
      const a = this.arbAgg[p.coin] ?? (this.arbAgg[p.coin] = newArbAgg());
      a.total++;
      a.minSumTotal += p.arb.minSum;
      if (p.arb.minSum <= 0.99 + 1e-9) {
        a.h99++;
        a.sec99Sum += p.arb.sec99 ?? 0;
        a.size99Sum += p.arb.size99 ?? 0;
      }
      if (p.arb.minSum <= 0.98 + 1e-9) a.h98++;
      if (p.arb.minSum <= 0.97 + 1e-9) a.h97++;
      if (p.arb.minSum <= 0.95 + 1e-9) a.h95++;
    }

    this.dropPending(slug);
    this.resolvedCount++;
  }

  async checkResolutions(): Promise<void> {
    const now = Date.now();
    const toCheck: string[] = [];
    for (const [slug, p] of this.pending) {
      const age = now - p.closeTimeMs;
      if (age >= RESOLVE_GIVE_UP_MS) {
        this.dropPending(slug);
        continue;
      }
      if (age >= RESOLVE_CHECK_DELAY_SEC * 1000) toCheck.push(slug);
    }

    let changed = false;
    for (const slug of toCheck) {
      const winner = await resolveWinner(slug);
      if (winner === null) continue;
      this.settle(slug, winner);
      changed = true;
    }
    this.settlePairs();
    if (changed) this.saveState();
  }

  // ───── отчёты ─────

  private collect(idea: string): { combo: string; coin: string; w: number; t: number; roi: number; px: number }[] {
    const out: { combo: string; coin: string; w: number; t: number; roi: number; px: number }[] = [];
    for (const [key, s] of Object.entries(this.agg)) {
      if (!key.startsWith(`${idea}|`)) continue;
      const parts = key.split("|");
      out.push({ combo: parts[1], coin: parts[2], w: s.w, t: s.t, roi: s.roi, px: s.px ?? 0 });
    }
    return out;
  }

  private topLines(list: { combo: string; w: number; t: number; roi: number }[], n: number): string[] {
    const top = list
      .filter((e) => e.t >= MIN_TRADES_FOR_TOP)
      .sort((a, b) => b.w / b.t - a.w / a.t || b.t - a.t)
      .slice(0, n);
    if (top.length === 0) return [`  пока недостаточно данных (нужно ≥${MIN_TRADES_FOR_TOP} записей на комбинацию)`];
    return top.map(
      (e, i) => `  ${i + 1}. ${e.combo} — ${winPct(e.w, e.t)} (${e.w}/${e.t}), доходность ${roiLabel(e.roi, e.t)}`,
    );
  }

  private pooled(idea: string): PooledRow[] {
    const m = new Map<string, PooledRow>();
    for (const s of this.collect(idea)) {
      const cur = m.get(s.combo) ?? { combo: s.combo, w: 0, t: 0, roi: 0, px: 0 };
      cur.w += s.w;
      cur.t += s.t;
      cur.roi += s.roi;
      cur.px += s.px;
      m.set(s.combo, cur);
    }
    return [...m.values()];
  }

  private buildIdeaReport(idea: string, title: string, note: string): string {
    const stats = this.collect(idea);
    const total = stats.reduce((a, s) => a + s.t, 0);
    const lines: string[] = [];
    lines.push(`<b>📊 ${title}</b>`);
    lines.push(note);
    lines.push("Цена входа проверяется живым стаканом (REST) перед записью.");
    lines.push(`Записей входа по всем комбинациям (после резолва): ${total}`);
    lines.push("");
    lines.push("<b>── Все монеты вместе: топ-5 ──</b>");
    lines.push(...this.topLines(this.pooled(idea), 5));
    lines.push("");
    for (const coin of INCLUDED_COINS) {
      lines.push(`<b>── ${coin}: топ-5 ──</b>`);
      lines.push(...this.topLines(stats.filter((s) => s.coin === coin), 5));
      lines.push("");
    }
    lines.push("Доходность — средний результат на $1 ставки (без комиссий).");
    return lines.join("\n");
  }

  buildMarginReport(): string {
    return this.buildIdeaReport(
      "margin",
      "Идея «Запас» (z вместо порога)",
      "z = движение монеты в сторону токена ÷ (типичное 5-мин движение × √(осталось/300)). Один параметр на все монеты. Цена токена 0.97-0.98, вход по ask.",
    );
  }

  buildCheapReport(): string {
    return this.buildIdeaReport(
      "cheap",
      "Идея «Недооценка»",
      "Монета уже прошла порог в сторону токена, а ask токена 0.85-0.96. Формат: порог / окно / диапазон цены ask.",
    );
  }

  buildPanicReport(): string {
    return this.buildIdeaReport(
      "panic",
      "Идея «Паника без разворота»",
      "Токен был ≥0.97, потом ask просел до 0.85-0.95, а монета всё ещё за порогом движения. Формат: порог / окно / диапазон цены ask.",
    );
  }

  buildImpulseReport(): string {
    return this.buildIdeaReport(
      "impulse",
      "Идея «Импульс последних секунд»",
      "Монета прошла N% за последние 15 или 30с в сторону токена, при этом от открытия окна она тоже в эту сторону; ask токена 0.60-0.98. Формат: рывок за N секунд / окно.",
    );
  }

  // ───── «Модель цены» ─────

  private toModelRows(rows: PooledRow[]): ModelRow[] {
    return rows
      .filter((r) => r.t > 0)
      .map((r) => {
        const wr = r.w / r.t;
        const avgPx = r.px / r.t;
        const lo = wilsonLower(r.w, r.t);
        const verdict: ModelRow["verdict"] = lo > avgPx ? "✅" : wr > avgPx ? "⚠️" : "❌";
        return { ...r, wr, avgPx, lo, edge: wr - avgPx, roiAvg: r.roi / r.t, verdict };
      });
  }

  private modelLine(idx: number | null, r: ModelRow): string {
    return (
      `  ${idx !== null ? `${idx}. ` : ""}${r.combo} → ${r.w}/${r.t} (${winPct(r.w, r.t)}), ` +
      `надёжно ${(r.lo * 100).toFixed(0)}%, цена ${r.avgPx.toFixed(2)}, запас ${signedPct(r.edge)}, ` +
      `прибыль ${roiLabel(r.roi, r.t)} ${r.verdict}`
    );
  }

  buildModelReport(): string {
    const all = this.pooled("model");
    const total = all.reduce((a, s) => a + s.t, 0);
    const allRows = this.toModelRows(all);
    const rows = allRows.filter((r) => r.t >= MODEL_MIN_TRADES);

    const lines: string[] = [];
    lines.push("<b>📊 Модель цены</b>");
    lines.push(`Рынков обработано: ${this.resolvedCount} | Записей входа: ${total}`);
    lines.push("Цена входа = живой ask (проверяется REST перед записью). Держим до резолва.");
    lines.push("");
    lines.push("Как читать:");
    lines.push(" • Сила — во сколько раз монета прошла больше обычного для оставшегося времени (1 = обычное, 3+ = очень сильно)");
    lines.push(" • Цена — средняя цена токена на входе");
    lines.push(" • Запас — реальный винрейт минус цена. Плюс = выгодно");
    lines.push(" • Надёжно — осторожная оценка винрейта с поправкой на малую выборку");
    lines.push(" • ✅ надёжно выше цены (выгодно даже по осторожной оценке); ⚠️ винрейт выше цены, но данных мало, может быть удача; ❌ невыгодно");
    lines.push(" • Один рынок может попасть в несколько комбинаций по мере того, как идёт время и меняется цена");
    lines.push("");

    // топ-10
    lines.push(`<b>── Все монеты: лучшие 10 комбинаций (минимум ${MODEL_MIN_TRADES} записей) ──</b>`);
    const best = [...rows].sort((a, b) => b.roiAvg - a.roiAvg || b.t - a.t).slice(0, 10);
    if (best.length === 0) lines.push(`  пока недостаточно данных (нужно ≥${MODEL_MIN_TRADES} записей на комбинацию)`);
    else best.forEach((r, i) => lines.push(this.modelLine(i + 1, r)));
    lines.push("");

    // ловушки
    lines.push("<b>── Ловушки: худшие 5 ──</b>");
    const worst = rows
      .filter((r) => r.roiAvg < 0)
      .sort((a, b) => a.roiAvg - b.roiAvg || b.t - a.t)
      .slice(0, 5);
    if (worst.length === 0) lines.push("  пока нет убыточных комбинаций с достаточной выборкой");
    else worst.forEach((r, i) => lines.push(this.modelLine(i + 1, r)));
    lines.push("");

    // по монетам
    lines.push(`<b>── По монетам: лучшая комбинация (минимум ${MODEL_MIN_COIN} записей) ──</b>`);
    const stats = this.collect("model");
    for (const coin of INCLUDED_COINS) {
      const coinRows = this.toModelRows(stats.filter((s) => s.coin === coin)).filter((r) => r.t >= MODEL_MIN_COIN);
      const top = coinRows.sort((a, b) => b.roiAvg - a.roiAvg || b.t - a.t)[0];
      lines.push(top ? `  ${COIN_SHORT[coin]}:${this.modelLine(null, top).slice(1)}` : `  ${COIN_SHORT[coin]}: пока мало данных`);
    }
    lines.push("");

    // сравнение с профилем 6
    lines.push("<b>── Сравнение с тем, что бот уже делает ──</b>");
    let bw = 0;
    let bt = 0;
    let br = 0;
    for (const s of this.collect("bot6")) {
      bw += s.w;
      bt += s.t;
      br += s.roi;
    }
    lines.push(
      bt > 0
        ? `  Профиль 6 «Запас» (цена 0.97-0.98): ${bw}/${bt} (${winPct(bw, bt)}), прибыль ${roiLabel(br, bt)}`
        : "  Профиль 6 «Запас» (цена 0.97-0.98): пока нет данных",
    );
    const good = rows.filter((r) => r.verdict === "✅");
    if (good.length === 0) {
      lines.push("  Модель, комбинации ✅: пока нет — нужно больше данных");
    } else {
      let gw = 0;
      let gt = 0;
      let gr = 0;
      let outside = 0;
      for (const r of good) {
        gw += r.w;
        gt += r.t;
        gr += r.roi;
        const upper = parseFloat((r.combo.split(" / ")[2] ?? "").split("-")[1] ?? "1");
        if (upper <= 0.97 + 1e-9) outside += r.t;
      }
      lines.push(`  Модель, только ✅: ${good.length} комбинаций, ${gw}/${gt} (${winPct(gw, gt)}), прибыль ${roiLabel(gr, gt)}`);
      lines.push(`  из них записей с ценой ниже 0.97 (то, чего бот сейчас не берёт): ${outside}`);
      lines.push(`  Частота ✅-записей: ${((100 * gt) / Math.max(this.resolvedCount, 1)).toFixed(1)} на 100 рынков`);
    }
    lines.push("");

    // срезы по каждой оси (все комбинации, без минимума)
    const group = (title: string, idx: number, order: string[]): void => {
      lines.push(`<b>── ${title} ──</b>`);
      let any = false;
      for (const label of order) {
        let w = 0;
        let t = 0;
        let roi = 0;
        let px = 0;
        for (const r of allRows) {
          if (r.combo.split(" / ")[idx] !== label) continue;
          w += r.w;
          t += r.t;
          roi += r.roi;
          px += r.px;
        }
        if (t === 0) continue;
        any = true;
        lines.push(
          `  ${label}: ${w}/${t} (${winPct(w, t)}), цена ${(px / t).toFixed(2)}, запас ${signedPct(w / t - px / t)}, прибыль ${roiLabel(roi, t)}`,
        );
      }
      if (!any) lines.push("  пока нет данных");
      lines.push("");
    };
    group("Срез по цене токена", 2, MODEL_PRICE_BANDS.map(priceLabel));
    group("Срез по времени до закрытия", 1, MODEL_SEC_BANDS.map(secLabel));
    group("Срез по силе движения", 0, MODEL_Z_BANDS.map(zLabel));

    lines.push("Прибыль — средний результат на $1 ставки (без комиссий).");
    return lines.join("\n");
  }

  // ───── «Пила» ─────

  buildSawReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Фильтр «Пила»</b>");
    lines.push(
      `Пересечение = монета перешла через цену открытия окна (была выше — стала ниже, или наоборот). Считаем за последние ${SAW_LOOKBACK_SEC / 60} мин до входа, замер каждые ${SAW_STEP_SEC}с (очень быстрые пересечения можно пропустить, число — нижняя оценка).`,
    );
    lines.push("Берётся самый ранний вход профиля на каждую сторону рынка. Держим до резолва, цена входа = живой ask.");
    lines.push("");

    const get = (key: string): Stat => this.saw[key] ?? { w: 0, t: 0, roi: 0 };

    const filterLines = (buckets: Stat[]): string[] => {
      const out: string[] = [];
      const tot = buckets.reduce((a, s) => ({ w: a.w + s.w, t: a.t + s.t, roi: a.roi + s.roi }), { w: 0, t: 0, roi: 0 });
      if (tot.t === 0) return out;
      const baseRoi = tot.roi / tot.t;
      for (const cut of [1, 2, 3]) {
        let kw = 0;
        let kt = 0;
        let kr = 0;
        let cw = 0;
        let ct = 0;
        for (let n = 0; n < buckets.length; n++) {
          const s = buckets[n];
          if (n < cut) {
            kw += s.w;
            kt += s.t;
            kr += s.roi;
          } else {
            cw += s.w;
            ct += s.t;
          }
        }
        if (ct === 0) {
          out.push(`  не входить при ${cut}+ пересечениях: ничего не отсекает`);
          continue;
        }
        const helps = kt > 0 && kr / kt > baseRoi && ct - cw > cw;
        out.push(
          `  не входить при ${cut}+: остаётся ${kw}/${kt} (${winPct(kw, kt)}), прибыль ${roiLabel(kr, kt)}; ` +
            `отсекли ${ct} (побед ${cw}, проигрышей ${ct - cw}) ${helps ? "✅ помогает" : "— не помогает"}`,
        );
      }
      return out;
    };

    for (const prof of BOT_PROFILES) {
      const buckets = SAW_BUCKETS.map((n) => get(`bot${prof.id}|x${n}`));
      const tot = buckets.reduce((a, s) => ({ w: a.w + s.w, t: a.t + s.t, roi: a.roi + s.roi }), { w: 0, t: 0, roi: 0 });
      lines.push(`<b>── Профиль ${prof.id}${prof.id === 6 ? " «Запас»" : prof.id === 7 ? " «Модель»" : ""} (всего ${tot.t} входов, ${winPct(tot.w, tot.t)}, прибыль ${roiLabel(tot.roi, tot.t)}) ──</b>`);
      if (tot.t === 0) {
        lines.push("  пока нет данных");
        lines.push("");
        continue;
      }
      SAW_BUCKETS.forEach((n, i) => {
        const s = buckets[i];
        lines.push(
          `  пересечений ${sawLabel(n)}: ${s.w}/${s.t} (${winPct(s.w, s.t)}), прибыль ${roiLabel(s.roi, s.t)}${s.t > 0 && s.t < 10 ? " (мало данных)" : ""}`,
        );
      });
      lines.push(...filterLines(buckets));
      lines.push("");
    }

    // все профили вместе
    const sumBuckets: Stat[] = SAW_BUCKETS.map((n) => {
      const acc: Stat = { w: 0, t: 0, roi: 0 };
      for (const prof of BOT_PROFILES) {
        const s = get(`bot${prof.id}|x${n}`);
        acc.w += s.w;
        acc.t += s.t;
        acc.roi += s.roi;
      }
      return acc;
    });
    lines.push("<b>── Все профили вместе ──</b>");
    lines.push("(одна и та же ситуация может учитываться в нескольких профилях сразу)");
    if (sumBuckets.every((s) => s.t === 0)) {
      lines.push("  пока нет данных");
    } else {
      SAW_BUCKETS.forEach((n, i) => {
        const s = sumBuckets[i];
        lines.push(`  пересечений ${sawLabel(n)}: ${s.w}/${s.t} (${winPct(s.w, s.t)}), прибыль ${roiLabel(s.roi, s.t)}`);
      });
      lines.push(...filterLines(sumBuckets));
    }
    lines.push("");

    // по монетам
    lines.push("<b>── По монетам (все профили вместе) ──</b>");
    for (const coin of INCLUDED_COINS) {
      const parts: string[] = [];
      for (const n of SAW_BUCKETS) {
        const s = get(`all|${coin}|x${n}`);
        if (s.t === 0) continue;
        parts.push(`${sawLabel(n)}: ${s.w}/${s.t} ${winPct(s.w, s.t)}`);
      }
      lines.push(`  ${COIN_SHORT[coin]}: ${parts.length > 0 ? parts.join(" | ") : "пока нет данных"}`);
    }
    lines.push("");
    lines.push("Прибыль — средний результат на $1 ставки (без комиссий). «Помогает» = после отсечения прибыль выше, и отсекли больше проигрышей, чем побед.");
    return lines.join("\n");
  }

  // ───── «Пара BTC+ETH»: отчёт ─────

  private parsePk(pk: string): { wB: number; mB: number; wE: number; mE: number; bi: number } {
    const [a, b, c, d, e] = pk.split(":");
    return { wB: Number(a), mB: Number(b), wE: Number(c), mE: Number(d), bi: Number(e) };
  }

  private pairLabel(pk: string): string {
    const k = this.parsePk(pk);
    const band = PAIR_BANDS[k.bi];
    return `BTC ${k.wB}с/${pctLabel(k.mB)} + ETH ${k.wE}с/${pctLabel(k.mE)}, цена ${band[0].toFixed(2)}-${band[1].toFixed(2)}`;
  }

  buildPairReport(): string {
    const sg = (key: string): Stat => this.pairAgg[key] ?? { w: 0, t: 0, roi: 0, px: 0 };
    const pks = new Set<string>();
    for (const key of Object.keys(this.pairAgg)) {
      if (key.startsWith("_sit")) continue;
      pks.add(key.split("|")[0]);
    }

    interface PairRow {
      pk: string;
      t: number;
      both: number;
      bothLost: number;
      oneLost: number;
      b: Stat;
      e: Stat;
      roi: number;
      roiAvg: number;
      avgPx: number;
      lo: number;
      verdict: "✅" | "⚠️" | "❌";
      soloB?: Stat;
      soloE?: Stat;
    }

    const rows: PairRow[] = [];
    for (const pk of pks) {
      const pr = sg(`${pk}|pair`);
      if (pr.t === 0) continue;
      const b = sg(`${pk}|btc`);
      const e = sg(`${pk}|eth`);
      const bl = sg(`${pk}|bothlost`);
      const legsT = b.t + e.t;
      const legsW = b.w + e.w;
      const avgPx = legsT > 0 ? ((b.px ?? 0) + (e.px ?? 0)) / legsT : 0;
      const lo = wilsonLower(legsW, legsT);
      const k = this.parsePk(pk);
      const verdict: PairRow["verdict"] = lo > avgPx ? "✅" : legsT > 0 && legsW / legsT > avgPx ? "⚠️" : "❌";
      rows.push({
        pk,
        t: pr.t,
        both: pr.w,
        bothLost: bl.w,
        oneLost: pr.t - pr.w - bl.w,
        b,
        e,
        roi: pr.roi,
        roiAvg: pr.roi / pr.t,
        avgPx,
        lo,
        verdict,
        soloB: this.agg[`psolo|${k.wB}:${k.mB}:${k.bi}|${PAIR_COINS[0]}`],
        soloE: this.agg[`psolo|${k.wE}:${k.mE}:${k.bi}|${PAIR_COINS[1]}`],
      });
    }

    const rowLines = (i: number, r: PairRow): string[] => {
      const out: string[] = [`  ${i}. ${this.pairLabel(r.pk)}`];
      out.push(
        `     пар: ${r.t} | обе выиграли: ${r.both} (${winPct(r.both, r.t)}) | одна проиграла: ${r.oneLost} | обе проиграли: ${r.bothLost}`,
      );
      out.push(
        `     BTC: ${r.b.w}/${r.b.t} (${winPct(r.b.w, r.b.t)}), ETH: ${r.e.w}/${r.e.t} (${winPct(r.e.w, r.e.t)}) | надёжно ${(r.lo * 100).toFixed(0)}%, цена ${r.avgPx.toFixed(2)} | прибыль на $1 с ноги: ${roiLabel(r.roi, r.t)} ${r.verdict}`,
      );
      if (r.soloB && r.soloE) {
        out.push(
          `     те же триггеры по одиночке: BTC ${r.soloB.w}/${r.soloB.t} (${winPct(r.soloB.w, r.soloB.t)}), ETH ${r.soloE.w}/${r.soloE.t} (${winPct(r.soloE.w, r.soloE.t)})`,
        );
      }
      return out;
    };

    const lines: string[] = [];
    lines.push("<b>📊 Пара BTC + ETH</b>");
    lines.push(`Пар ждёт резолва: ${this.pairs.length} | Комбинаций с данными: ${rows.length}`);
    const sitPair = sg("_sit|pair");
    const sitB = sg("_sit|btc");
    const sitE = sg("_sit|eth");
    if (sitPair.t > 0) {
      const eitherWon = sitB.w + sitE.w - sitPair.w;
      const bothLostN = sitPair.t - eitherWon;
      const oneLostN = sitPair.t - sitPair.w - bothLostN;
      lines.push(
        `<b>Настоящих ситуаций (без дублей): ${sitPair.t}</b> — обе выиграли ${sitPair.w} (${winPct(sitPair.w, sitPair.t)}), одна проиграла ${oneLostN}, обе проиграли ${bothLostN}. BTC ${sitB.w}/${sitB.t}, ETH ${sitE.w}/${sitE.t}. (счёт идёт с момента этого обновления)`,
      );
    } else {
      lines.push("Настоящих ситуаций (без дублей): счёт только начался, пока 0");
    }
    lines.push(
      "Покупаем оба токена, когда у обеих монет одновременно сработал свой триггер (окно + % движения), обе идут в одну сторону, цены токенов в коридоре. Цены по живому стакану, держим до резолва.",
    );
    lines.push("");
    lines.push("Как читать:");
    lines.push(" • Название комбинации: окно/процент для BTC + окно/процент для ETH, коридор цены токенов");
    lines.push(" • Обе выиграли / одна проиграла / обе проиграли — исходы пары");
    lines.push(" • Надёжно — осторожная оценка винрейта ног с поправкой на малую выборку");
    lines.push(" • ✅ надёжно выше цены; ⚠️ винрейт выше цены, но данных мало; ❌ невыгодно");
    lines.push(" • «По одиночке» — та же монета с тем же триггером без условия на вторую монету");
    lines.push(" • Одна и та же пара может попасть в несколько комбинаций сразу");
    lines.push("");

    const ok = rows.filter((r) => r.t >= PAIR_MIN_TRADES);

    lines.push(`<b>── Лучшие 10 комбинаций (минимум ${PAIR_MIN_TRADES} пар) ──</b>`);
    const best = [...ok].sort((a, b) => b.roiAvg - a.roiAvg || b.t - a.t).slice(0, 10);
    if (best.length === 0) lines.push(`  пока недостаточно данных (нужно ≥${PAIR_MIN_TRADES} пар на комбинацию)`);
    else best.forEach((r, i) => lines.push(...rowLines(i + 1, r)));
    lines.push("");

    lines.push("<b>── Ловушки: худшие 5 ──</b>");
    const worst = ok.filter((r) => r.roiAvg < 0).sort((a, b) => a.roiAvg - b.roiAvg || b.t - a.t).slice(0, 5);
    if (worst.length === 0) lines.push("  пока нет убыточных комбинаций с достаточной выборкой");
    else worst.forEach((r, i) => lines.push(...rowLines(i + 1, r)));
    lines.push("");

    // даёт ли «оба сразу» прирост винрейта по сравнению с одиночными входами
    lines.push("<b>── Помогает ли условие «оба сразу» ──</b>");
    let pw = 0;
    let pt = 0;
    let sw = 0;
    let st = 0;
    for (const r of ok) {
      if (!r.soloB || !r.soloE) continue;
      pw += r.b.w + r.e.w;
      pt += r.b.t + r.e.t;
      sw += r.soloB.w + r.soloE.w;
      st += r.soloB.t + r.soloE.t;
    }
    if (pt === 0 || st === 0) {
      lines.push("  пока недостаточно данных");
    } else {
      lines.push(`  ноги внутри пар: ${pw}/${pt} (${winPct(pw, pt)})`);
      lines.push(`  те же триггеры по одиночке: ${sw}/${st} (${winPct(sw, st)})`);
      lines.push(`  входов в парах меньше в ${(st / pt).toFixed(1)} раза — столько сделок теряем ради фильтра`);
    }
    lines.push("(считается по комбинациям с достаточной выборкой, комбинации перекрываются — цифры ориентировочные)");
    lines.push("");

    // срезы по каждой настройке
    const group = (title: string, keyOf: (pk: string) => string, order: string[]): void => {
      lines.push(`<b>── ${title} ──</b>`);
      let any = false;
      for (const label of order) {
        let t = 0;
        let both = 0;
        let lw = 0;
        let lt = 0;
        let roi = 0;
        for (const r of rows) {
          if (keyOf(r.pk) !== label) continue;
          t += r.t;
          both += r.both;
          lw += r.b.w + r.e.w;
          lt += r.b.t + r.e.t;
          roi += r.roi;
        }
        if (t === 0) continue;
        any = true;
        lines.push(
          `  ${label}: пар ${t}, обе выиграли ${both} (${winPct(both, t)}), ноги ${lw}/${lt} (${winPct(lw, lt)}), прибыль ${roiLabel(roi, t)}`,
        );
      }
      if (!any) lines.push("  пока нет данных");
      lines.push("");
    };
    const bandLabel = (bi: number): string => `${PAIR_BANDS[bi][0].toFixed(2)}-${PAIR_BANDS[bi][1].toFixed(2)}`;
    group("Срез по цене токенов", (pk) => bandLabel(this.parsePk(pk).bi), PAIR_BANDS.map((_, i) => bandLabel(i)));
    group("Срез по окну BTC", (pk) => `${this.parsePk(pk).wB}с`, PAIR_WINDOWS.map((w) => `${w}с`));
    group("Срез по окну ETH", (pk) => `${this.parsePk(pk).wE}с`, PAIR_WINDOWS.map((w) => `${w}с`));
    group("Срез по движению BTC", (pk) => pctLabel(this.parsePk(pk).mB), PAIR_MOVES.map((m) => pctLabel(m)));
    group("Срез по движению ETH", (pk) => pctLabel(this.parsePk(pk).mE), PAIR_MOVES.map((m) => pctLabel(m)));

    lines.push("Прибыль — средний результат на $1 ставки с каждой ноги (без комиссий).");
    return lines.join("\n");
  }

  buildArbReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Идея «Арбитраж Up+Down»</b>");
    lines.push(
      "Раз в 5 секунд берём живой стакан обеих сторон (REST) и считаем сумму лучших ask: ≤ 0.99 / 0.98 / 0.97 / 0.95. Короткие окна между замерами можем пропустить, поэтому частота — нижняя оценка. Количество акций — по лучшему уровню стакана.",
    );
    lines.push("");

    const all = newArbAgg();
    const rowFor = (label: string, a: ArbAgg): string[] => {
      if (a.total === 0) return [`<b>── ${label} ──</b>`, "  пока нет данных", ""];
      const pct = (n: number) => `${n} (${((100 * n) / a.total).toFixed(1)}%)`;
      const sec = a.h99 > 0 ? `, в среднем за ${(a.sec99Sum / a.h99).toFixed(0)}с до закрытия` : "";
      const size = a.h99 > 0 ? `, доступно в среднем ${(a.size99Sum / a.h99).toFixed(0)} акций` : "";
      return [
        `<b>── ${label} ──</b>`,
        `  рынков: ${a.total}, средняя минимальная сумма: ${(a.minSumTotal / a.total).toFixed(3)}`,
        `  сумма ≤ 0.99: ${pct(a.h99)}${sec}${size}`,
        `  сумма ≤ 0.98: ${pct(a.h98)}`,
        `  сумма ≤ 0.97: ${pct(a.h97)}`,
        `  сумма ≤ 0.95: ${pct(a.h95)}`,
        "",
      ];
    };

    for (const coin of INCLUDED_COINS) {
      const a = this.arbAgg[coin];
      if (a) {
        all.total += a.total;
        all.minSumTotal += a.minSumTotal;
        all.h99 += a.h99;
        all.h98 += a.h98;
        all.h97 += a.h97;
        all.h95 += a.h95;
        all.sec99Sum += a.sec99Sum;
        all.size99Sum += a.size99Sum;
      }
    }
    lines.push(...rowFor("Все монеты", all));
    for (const coin of INCLUDED_COINS) {
      lines.push(...rowFor(coin, this.arbAgg[coin] ?? newArbAgg()));
    }
    return lines.join("\n");
  }

  private ideaLabel(idea: string): string {
    switch (idea) {
      case "margin":
        return "Запас";
      case "cheap":
        return "Недооценка";
      case "panic":
        return "Паника без разворота";
      case "impulse":
        return "Импульс";
      case "model":
        return "Модель цены";
      default:
        return idea.startsWith("bot") ? `Профиль ${idea.slice(3)} (бот)` : idea;
    }
  }

  buildConfirmReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Подтверждение другими монетами</b>");
    lines.push(
      "Сколько ДРУГИХ монет (из 4) в момент входа идут в ту же сторону не менее чем на 0.05% / 0.10% от открытия окна. Формат: подтвердили монет: выигрыш/всего, винрейт. Одна запись на ситуацию (самый ранний вход идеи).",
    );
    lines.push("");

    const ideas = ["margin", "cheap", "panic", "impulse", "model", "bot1", "bot2", "bot3", "bot4", "bot5", "bot6", "bot7"];
    for (const idea of ideas) {
      lines.push(`<b>── ${this.ideaLabel(idea)} ──</b>`);
      let any = false;
      for (const [lvl, label] of [
        ["c05", "порог 0.05%"],
        ["c10", "порог 0.10%"],
      ] as const) {
        const parts: string[] = [];
        for (let n = 0; n <= 4; n++) {
          const s = this.conf[`${idea}|${lvl}|${n}`];
          if (!s || s.t === 0) continue;
          parts.push(`${n}: ${s.w}/${s.t} ${winPct(s.w, s.t)}`);
        }
        if (parts.length > 0) {
          any = true;
          lines.push(`  ${label} → ${parts.join(" | ")}`);
        }
      }
      if (!any) lines.push("  пока нет данных");
      lines.push("");
    }
    return lines.join("\n");
  }

  buildProfilesReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Профили бота и согласие профилей</b>");
    lines.push(
      `Моделирование ${BOT_PROFILES.length} профилей бота: вход по живому ask, потолок покупки как в боте, держим до резолва.`,
    );
    lines.push("");
    lines.push("<b>── Каждый профиль отдельно (выигрыш/всего, винрейт, доходность на $1) ──</b>");
    for (const prof of BOT_PROFILES) {
      const idea = `bot${prof.id}`;
      const stats = this.collect(idea);
      const parts: string[] = [];
      let tw = 0;
      let tt = 0;
      let tr = 0;
      for (const coin of INCLUDED_COINS) {
        const s = stats.find((x) => x.coin === coin);
        if (!s) continue;
        tw += s.w;
        tt += s.t;
        tr += s.roi;
        parts.push(`${COIN_SHORT[coin]} ${s.w}/${s.t} ${winPct(s.w, s.t)}`);
      }
      lines.push(`  Профиль ${prof.id}: всего ${tw}/${tt} ${winPct(tw, tt)}, доходность ${roiLabel(tr, tt)}`);
      if (parts.length > 0) lines.push(`     ${parts.join(" | ")}`);
    }
    lines.push("");

    lines.push("<b>── Согласие: сколько профилей дали сигнал на одну сторону рынка ──</b>");
    lines.push("(считаются сигналы за весь рынок до закрытия; доходность — по цене самого раннего входа)");
    let anyN = false;
    for (let n = 1; n <= BOT_PROFILES.length; n++) {
      const s = this.agree[`n|${n}`];
      if (!s || s.t === 0) continue;
      anyN = true;
      lines.push(`  профилей: ${n} → ${s.w}/${s.t} ${winPct(s.w, s.t)}, доходность ${roiLabel(s.roi, s.t)}`);
    }
    if (!anyN) lines.push("  пока нет данных");
    lines.push("");

    lines.push("<b>── Конкретные сочетания профилей (минимум 5 случаев) ──</b>");
    const sets = Object.entries(this.agree)
      .filter(([k, s]) => k.startsWith("set|") && s.t >= 5)
      .sort((a, b) => b[1].t - a[1].t)
      .slice(0, 12);
    if (sets.length === 0) {
      lines.push("  пока недостаточно данных");
    } else {
      for (const [k, s] of sets) {
        lines.push(`  профили ${k.slice(4)} → ${s.w}/${s.t} ${winPct(s.w, s.t)}, доходность ${roiLabel(s.roi, s.t)}`);
      }
    }
    return lines.join("\n");
  }

  buildSummaryReport(): string {
    const lines: string[] = [];
    lines.push("<b>🧪 LAB: сводка</b>");
    lines.push(
      `Рынков обработано: ${this.resolvedCount} (увидено всего: ${this.marketsSeenCount}), ждут резолва: ${this.pending.size}`,
    );
    lines.push("");

    const ideas: [string, string][] = [
      ["margin", "Запас"],
      ["cheap", "Недооценка"],
      ["panic", "Паника без разворота"],
      ["impulse", "Импульс"],
    ];
    for (const [idea, label] of ideas) {
      lines.push(`<b>── ${label}: топ-3 (все монеты вместе) ──</b>`);
      lines.push(...this.topLines(this.pooled(idea), 3));
      lines.push("");
    }

    // модель цены — коротко
    const modelRows = this.toModelRows(this.pooled("model")).filter((r) => r.t >= MODEL_MIN_TRADES);
    const modelGood = modelRows.filter((r) => r.verdict === "✅").length;
    const modelMaybe = modelRows.filter((r) => r.verdict === "⚠️").length;
    lines.push("<b>── Модель цены ──</b>");
    lines.push(
      modelRows.length > 0
        ? `  комбинаций с ≥${MODEL_MIN_TRADES} записями: ${modelRows.length}, из них ✅ ${modelGood}, ⚠️ ${modelMaybe}  (подробно: "лаб модель")`
        : `  пока недостаточно данных (нужно ≥${MODEL_MIN_TRADES} записей на комбинацию)`,
    );
    lines.push("");

    // пара — коротко
    let pairCombos = 0;
    let pairGood = 0;
    for (const key of Object.keys(this.pairAgg)) {
      if (!key.endsWith("|pair") || key.startsWith("_sit")) continue;
      const s = this.pairAgg[key];
      if (s.t < PAIR_MIN_TRADES) continue;
      pairCombos++;
      const pk = key.split("|")[0];
      const b = this.pairAgg[`${pk}|btc`];
      const e = this.pairAgg[`${pk}|eth`];
      if (!b || !e) continue;
      const lt = b.t + e.t;
      const avgPx = ((b.px ?? 0) + (e.px ?? 0)) / Math.max(lt, 1);
      if (wilsonLower(b.w + e.w, lt) > avgPx) pairGood++;
    }
    lines.push("<b>── Пара BTC+ETH ──</b>");
    lines.push(
      pairCombos > 0
        ? `  комбинаций с ≥${PAIR_MIN_TRADES} парами: ${pairCombos}, из них ✅ ${pairGood}  (подробно: "лаб пара")`
        : `  пока недостаточно данных (нужно ≥${PAIR_MIN_TRADES} пар на комбинацию)`,
    );
    lines.push("");

    const arbAll = Object.values(this.arbAgg).reduce(
      (acc, a) => ({ total: acc.total + a.total, h99: acc.h99 + a.h99 }),
      { total: 0, h99: 0 },
    );
    lines.push("<b>── Арбитраж (живые стаканы) ──</b>");
    lines.push(
      arbAll.total > 0
        ? `  сумма ask ≤ 0.99 была на ${arbAll.h99} из ${arbAll.total} рынков (${((100 * arbAll.h99) / arbAll.total).toFixed(1)}%)`
        : "  пока нет данных",
    );
    lines.push("");

    lines.push("<b>── Профили бота (моделирование) ──</b>");
    for (const prof of BOT_PROFILES) {
      let tw = 0;
      let tt = 0;
      let tr = 0;
      for (const s of this.collect(`bot${prof.id}`)) {
        tw += s.w;
        tt += s.t;
        tr += s.roi;
      }
      lines.push(`  Профиль ${prof.id}: ${tw}/${tt} ${winPct(tw, tt)}, доходность ${roiLabel(tr, tt)}`);
    }
    lines.push("");

    lines.push("<b>── Честность цен ──</b>");
    lines.push(
      this.staleSampled > 0
        ? `  замеров цены в памяти против живого стакана: ${this.staleSampled}, ask отличался на 0.02 и больше: ${this.staleShifted} (${((100 * this.staleShifted) / this.staleSampled).toFixed(1)}%)`
        : "  замеров пока нет",
    );
    const verifyTotal = this.verifyConfirmed + this.verifyRejected;
    lines.push(
      verifyTotal > 0
        ? `  проверок входов живым стаканом: ${verifyTotal}, подтвердились: ${this.verifyConfirmed}, отклонены: ${this.verifyRejected}`
        : "  проверок входов пока нет",
    );
    lines.push("");
    lines.push('Все команды: "лаб помощь"');
    return lines.join("\n");
  }

  buildHelp(): string {
    return [
      "<b>🧪 LAB — команды</b>",
      "лаб — сводка по всем идеям",
      "лаб запас — идея «Запас» (z вместо порога)",
      "лаб дешево — «Недооценка» (токен дешевле 0.96)",
      "лаб паника — «Паника без разворота»",
      "лаб импульс — «Импульс последних секунд»",
      "лаб арб — «Арбитраж Up+Down»",
      "лаб монеты — подтверждение другими монетами",
      "лаб профили — профили бота и согласие профилей",
      "лаб модель — «Модель цены» (где выгодно покупать вне 0.97-0.98)",
      "лаб пила — фильтр «Пила» (пересечения цены открытия)",
      "лаб пара — «Пара BTC+ETH» (покупка обеих, когда сработали оба триггера)",
      "лаб бинанс — второе мнение Binance (вошёл бы бот, если Binance уже развернулся)",
      "лаб лоси — вскрытие проигрышей: чем они отличаются от побед",
      "лаб согласие — винрейт в зависимости от числа профилей с сигналом",
      "лаб стоп — стоп-лосс: насколько проседает цена после входа",
      "лаб помощь — этот список",
    ].join("\n");
  }

  start(): void {
    this.loadState();
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => this.checkResolutions(), 30 * 1000);
    setInterval(() => this.sampleArb(), ARB_SAMPLE_MS);
    setInterval(() => this.saveState(), AUTOSAVE_INTERVAL_MS);
    setInterval(() => {
      console.log(
        `--- статус: апдейтов ${this.updateCount}, рынков обработано ${this.resolvedCount}, ждут резолва ${this.pending.size}, проверок входов ${this.verifyConfirmed + this.verifyRejected} ---`,
      );
    }, 60 * 1000);
  }
}

// ─────────────────────────── Telegram ───────────────────────────

const TELEGRAM_MAX_CHUNK = 3500;

function splitReportIntoChunks(report: string, maxLen: number = TELEGRAM_MAX_CHUNK): string[] {
  const lines = report.split("\n");
  const chunks: string[] = [];
  let current = "";
  for (const line of lines) {
    const candidate = current ? `${current}\n${line}` : line;
    if (candidate.length > maxLen && current) {
      chunks.push(current);
      current = line;
    } else {
      current = candidate;
    }
  }
  if (current) chunks.push(current);
  return chunks.length > 0 ? chunks : [""];
}

async function sendReportToTelegram(
  telegram: ReturnType<typeof createTelegramNotifier>,
  report: string,
): Promise<void> {
  const chunks = splitReportIntoChunks(report);
  for (let i = 0; i < chunks.length; i++) {
    const text = i === 0 ? chunks[i] : `<b>...продолжение (${i + 1}/${chunks.length})</b>\n\n${chunks[i]}`;
    try {
      await telegram?.send(text);
    } catch (err) {
      console.error(`[sendReport] ошибка отправки части ${i + 1}/${chunks.length}:`, (err as Error).message);
    }
  }
}

function normalizeCmd(raw: string): string {
  return raw.toLowerCase().replace(/ё/g, "е").replace(/@\w+/g, "").replace(/\s+/g, " ").trim();
}

const COMMANDS: Record<string, string[]> = {
  summary: ["лаб", "/lab"],
  margin: ["лаб запас", "/lab_margin"],
  cheap: ["лаб дешево", "/lab_cheap"],
  panic: ["лаб паника", "/lab_panic"],
  impulse: ["лаб импульс", "/lab_impulse"],
  arb: ["лаб арб", "/lab_arb"],
  confirm: ["лаб монеты", "/lab_coins"],
  profiles: ["лаб профили", "/lab_profiles"],
  model: ["лаб модель", "/lab_model"],
  saw: ["лаб пила", "/lab_saw"],
  pair: ["лаб пара", "/lab_pair"],
  binance: ["лаб бинанс", "/lab_binance"],
  losses: ["лаб лоси", "/lab_losses"],
  agree: ["лаб согласие", "/lab_agree"],
  stop: ["лаб стоп", "/lab_stop"],
  help: ["лаб помощь", "/lab_help"],
};

async function pollTelegramCommands(
  botToken: string,
  chatId: string,
  telegram: ReturnType<typeof createTelegramNotifier>,
  lab: ResearchLab,
): Promise<void> {
  let offset = 0;
  const apiUrl = `https://api.telegram.org/bot${botToken}/getUpdates`;

  for (;;) {
    try {
      const resp = await fetch(`${apiUrl}?offset=${offset}&timeout=25`);
      if (!resp.ok) {
        await new Promise((r) => setTimeout(r, 5000));
        continue;
      }
      const data = await resp.json();
      for (const update of data.result ?? []) {
        offset = update.update_id + 1;
        const msg = update.message;
        if (!msg?.text || String(msg.chat?.id) !== String(chatId)) continue;
        const text = normalizeCmd(msg.text);

        const cmd = Object.keys(COMMANDS).find((k) => COMMANDS[k].includes(text));
        if (!cmd) continue;
        console.log(`[telegram] команда: "${msg.text}" → ${cmd}`);

        let report: string;
        switch (cmd) {
          case "summary":
            report = lab.buildSummaryReport();
            break;
          case "margin":
            report = lab.buildMarginReport();
            break;
          case "cheap":
            report = lab.buildCheapReport();
            break;
          case "panic":
            report = lab.buildPanicReport();
            break;
          case "impulse":
            report = lab.buildImpulseReport();
            break;
          case "arb":
            report = lab.buildArbReport();
            break;
          case "confirm":
            report = lab.buildConfirmReport();
            break;
          case "profiles":
            report = lab.buildProfilesReport();
            break;
          case "model":
            report = lab.buildModelReport();
            break;
          case "saw":
            report = lab.buildSawReport();
            break;
          case "pair":
            report = lab.buildPairReport();
            break;
          case "binance":
            report = lab.buildBinanceReport();
            break;
          case "losses":
            report = lab.buildLossReport();
            break;
          case "agree":
            report = lab.buildAgreeReport();
            break;
          case "stop":
            report = lab.buildStopReport();
            break;
          default:
            report = lab.buildHelp();
        }
        await sendReportToTelegram(telegram, report);
      }
    } catch (err) {
      console.error("[telegram poll] ошибка:", (err as Error).message);
      await new Promise((r) => setTimeout(r, 5000));
    }
  }
}

// ─────────────────────────── Запуск ───────────────────────────

async function main() {
  console.log("=== LAB v2.4: исследовательский парсер идей запущен (не торгует, цены входа проверяются живым стаканом) ===");
  console.log(
    "Идеи: запас (z), недооценка, паника без разворота, импульс, арбитраж, подтверждение монетами, согласие профилей (7 профилей), модель цены, пила, пара BTC+ETH, Binance, лоси, стоп.",
  );

  btcPriceFeed.start();
  ethPriceFeed.start();
  solPriceFeed.start();
  xrpPriceFeed.start();
  dogePriceFeed.start();
  volTracker.start();
  binance.start();

  const logger = createLogger(false);
  const botToken = process.env.LAB_TELEGRAM_BOT_TOKEN ?? process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.LAB_TELEGRAM_CHAT_ID ?? process.env.TELEGRAM_CHAT_ID;
  const telegram = createTelegramNotifier(botToken, chatId, logger);

  const lab = new ResearchLab();
  lab.start();

  if (telegram && botToken && chatId) {
    console.log('Telegram включён. Команды начинаются со слова "лаб" (список: "лаб помощь").');
    pollTelegramCommands(botToken, chatId, telegram, lab);
  } else {
    console.log("Telegram не настроен — отчёты недоступны (задай LAB_TELEGRAM_BOT_TOKEN и LAB_TELEGRAM_CHAT_ID).");
  }

  const shutdown = () => {
    lab.saveState();
    process.exit(0);
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("Фатальная ошибка:", err);
  process.exit(1);
});
