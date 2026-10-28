/**
 * LAB v3.7 — исследовательский парсер идей. По умолчанию НЕ торгует; с LAB_TRADE=on реально покупает идею bot7b0 (профиль 9, $5) через clob.ts.
 * Работает отдельным процессом, свой файл состояния (research-lab-state.json),
 * свои команды в Telegram (все начинаются со слова "лаб").
 *
 * ГЛАВНОЕ В v3.0: СТАТИСТИКА СЧИТАЕТСЯ ТАК, ЧТОБЫ ЕЁ МОЖНО БЫЛО ПЕРЕНОСИТЬ В БОТА.
 *  Каждая комбинация/профиль записывает КАЖДЫЙ сигнал (без искусственных ограничений на число позиций), потому что
 *  лимиты бота (позиций в окне, открытых всего) — это управление риском, а не часть стратегии, и они искажают винрейт.
 *  Лимиты можно включить обратно константами MAX_PER_WINDOW / MAX_OPEN_TOTAL / ONE_PER_MARKET ниже.
 *   1) Binance-фильтр работает как в боте: это УСЛОВИЕ входа с ожиданием, а не метка;
 *   2) перед записью входа — живой стакан: ask не выше потолка коридора, живой bid не ниже низа коридора;
 *   3) цена входа = живой ask, итог только по официальному резолву.
 *
 * НОВОЕ В v3.2 (дополнение): темка работает и на 15-минутках (только она; остальные идеи по-прежнему считаются только по 5-минуткам) и
 *  получила режимы «контртренд» (покупка дешёвой стороны после рывка монеты против неё). Команды: «лаб темка» (5м) и «лаб темка 15» (15м).
 *
 * НОВОЕ В v3.2: «лаб темка» — скальпинг от начала пятиминутки. В начале окна «покупаем» Up и Down (по рынку по ask и лимиткой
 *  на уровнях 0.50 / 0.48 / 0.45 / 0.40 / 0.35 с ожиданием 60с и 180с) и «продаём» лимиткой на +2/+3/+4/+5/+7/+10 центов от входа.
 *  Не достигли цели до закрытия — держим до резолва. Общая статистика, по сторонам (Up/Down) и по каждой монете.
 *
 * НОВОЕ В v3.1 (файл состояния остаётся совместимым: старая статистика НЕ сбрасывается, новые идеи копятся с нуля):
 *  A) «лаб модель бинанс» — Binance-против на ВСЕЙ сетке модели (+ «доза-эффект»: чуть против / сильно против);
 *  B) Coinbase как второй источник цены (опрос REST раз в секунду): корзины Coinbase и «две биржи сразу»
 *     (для модели и для всех профилей бота) — «лаб биржи»;
 *  C) «лаб режим» — режим волатильности (σ за 2 мин / σ за 5 мин против обычной), часы суток, кластеры
 *     одновременных проигрышей в одном окне (признак для лосей);
 *  D) «лаб справедливая» — p = Φ(d / (σ·√t)), edge = p − ask, корзины edge, калибровка и проверка «растёт ли винрейт»;
 *  E) «лаб зараза» — у скольких соседних монет токен нашей стороны уже сыпется;
 *  F) «лаб траектория» — токен пришёл в коридор сверху (падал) или снизу (рос): ask сейчас − ask 15с назад.
 *  Все новые признаки пишутся к каждому базовому входу профилей бота и видны в «лаб лоси».
 *
 * Идеи и команды те же: запас, дешево, паника, импульс, арб, монеты, профили, модель, пила, пара, бинанс, лоси,
 * согласие, стоп, «лаб бот» + новые команды (см. «лаб помощь»).
 *
 * ВАЖНО: если меняешь настройки профилей/лимитов в боте — поменяй их и здесь (BOT_PROFILES, MAX_PER_WINDOW,
 * MAX_OPEN_TOTAL, LIVE_VARIANTS).
 *
 * Переменные окружения: LAB_TELEGRAM_BOT_TOKEN и LAB_TELEGRAM_CHAT_ID
 * (если не заданы — берутся TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID).
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
import { Side } from "@polymarket/clob-client-v2";
import { ClobService } from "./clob.js";
import { appendMarketRecord, compressOldDays, buildPage, buildExchangeSummary, MarketRec, RawTrade, TapeRow } from "./labAnalytics.js";

// ─────────────────────────── Общие настройки ───────────────────────────

// Где хранить файлы: постоянный диск Railway (RAILWAY_VOLUME_MOUNT_PATH) или папка из LAB_DATA_PATH, иначе рабочая папка.
const STORAGE_DIR = process.env.LAB_DATA_PATH || process.env.RAILWAY_VOLUME_MOUNT_PATH || process.cwd();
const STATE_FILE = path.resolve(STORAGE_DIR, "research-lab-state.json");
const LAB_DATA_DIR = path.resolve(STORAGE_DIR, "lab-data"); // v3.3: карточки рынков (лента цен + все сделки), без лимита по времени
const TAPE_MS = 5 * 1000; // лента цен: раз в 5с
const STATE_VERSION = 4; // v3.1 совместима с v3.0: новые поля читаются с запасными значениями
const AUTOSAVE_INTERVAL_MS = 60 * 1000;

const TARGET_WINDOW_MINUTES = 5;
// 15-минутные рынки наблюдаются ТОЛЬКО для «Темки» (остальные идеи подобраны под 5 минут и 15-минутки не видят).
// Если в логе [refresh] 15м рынков 0 — суффикс ниже не подошёл вашему cryptoMarketDiscovery.ts, поправьте его.
const TEMKA_EXTRA_WINDOWS = [15];
const TIMEFRAMES_TO_DISCOVER = [
  { suffixes: ["up-or-down-5m"], minutes: TARGET_WINDOW_MINUTES },
  { suffixes: ["up-or-down-15m"], minutes: 15 },
];

const MARKET_REFRESH_MS = 30 * 1000;
const RESOLVE_CHECK_DELAY_SEC = 180;
const RESOLVE_GIVE_UP_MS = 60 * 60 * 1000;
const GAMMA_HOST = "https://gamma-api.polymarket.com";
const CLOB_HOST = process.env.CLOB_HOST ?? "https://clob.polymarket.com";

const VERIFY_COOLDOWN_MS = 1000; // не чаще 1 проверки живым стаканом на токен раз в 1с (бот проверяет сразу на каждом тике)
const ARB_SAMPLE_MS = 5 * 1000; // замер арбитража живыми стаканами раз в 5с
const FETCH_TIMEOUT_MS = 4 * 1000;
const STALE_DIFF = 0.02; // насколько ask в памяти должен отличаться от живого, чтобы считать его устаревшим

const MIN_TRADES_FOR_TOP = 10; // минимум записей, чтобы комбинация попала в топ
const MAX_SETTLED_REMEMBERED = 3000;

// Ограничения бота (как settings.maxPerWindow / settings.maxOpenTotal в fastflip.ts). Применяются к КАЖДОЙ комбинации отдельно.
// 999 = лимита нет: статистика записывает каждый сигнал. Поставь 3 и 6, чтобы увидеть, как считал бы бот с лимитами.
const MAX_PER_WINDOW = 999;
const MAX_OPEN_TOTAL = 999;
// true — комбинация входит в рынок только один раз (в любую сторону); false — по разу в каждую сторону.
const ONE_PER_MARKET = false;
const RULES_TEXT =
  ONE_PER_MARKET || MAX_PER_WINDOW < 100 || MAX_OPEN_TOTAL < 100
    ? `${ONE_PER_MARKET ? "одна позиция на рынок, " : ""}${MAX_PER_WINDOW < 100 ? `не больше ${MAX_PER_WINDOW} в окне, ` : ""}${MAX_OPEN_TOTAL < 100 ? `не больше ${MAX_OPEN_TOTAL} открытых` : "без лимита открытых"}`
    : "без лимитов: записывается каждый сигнал";
// v3.6: «как бы покупает» — для идей отсюда LAB пишет в лог и в Telegram момент покупки (бумажная торговля, ордеров не шлёт),
// чтобы сверять с реальным ботом секунда в секунду. size — ставка в $, sizeRefPrice — цена, от которой бот считает число акций (нижняя граница коридора).
const PAPER_TRADE: Record<string, { size: number; sizeRefPrice: number; cap: number; minBid: number }> = {
  bot7b0: { size: 5, sizeRefPrice: 0.92, cap: 0.98, minBid: 0.92 }, // профиль 9 бота: «Модель — Binance против»
};
// v3.7: РЕАЛЬНАЯ торговля теми же идеями (PAPER_TRADE). Включается переменной LAB_TRADE=on (нужны PRIVATE_KEY, FUNDER_ADDRESS и т.д. как у бота).
// Правила — как у бота: одна позиция на рынок, не больше 3 в окне, не больше 6 открытых, не больше 60 в час.
const LIVE_TRADE_ENV = (process.env.LAB_TRADE ?? "off").toLowerCase() === "on";
const LIVE_MAX_PER_WINDOW = Number(process.env.LAB_MAX_PER_WINDOW ?? "3");
const LIVE_MAX_OPEN = Number(process.env.LAB_MAX_OPEN ?? "6");
const LIVE_QUOTA_PER_HOUR = Number(process.env.LAB_QUOTA_PER_HOUR ?? "60");
const LIVE_ATTEMPTS = 3; // сколько раз пробуем купить, если ордер не исполнился (FAK без встречных заявок)
const LIVE_RETRY_DELAY_MS = 700;
const LIVE_MIN_SEC_LEFT = 3; // не пытаемся покупать, если до закрытия меньше стольких секунд

function resolveFillLive(rawA: number, rawB: number, expectedPrice: number): { filledSize: number; avgPrice: number } {
  const cands: { shares: number; price: number }[] = [];
  for (const [shares, dollars] of [[rawA, rawB], [rawB, rawA]] as const) {
    if (shares <= 0) continue;
    const price = dollars / shares;
    if (price > 0 && price < 1) cands.push({ shares, price });
  }
  if (cands.length === 0) return { filledSize: 0, avgPrice: 0 };
  cands.sort((a, b) => Math.abs(a.price - expectedPrice) - Math.abs(b.price - expectedPrice));
  return { filledSize: cands[0].shares, avgPrice: cands[0].price };
}
const ABOVE_CAP_MAX_ASK = 0.995; // v3.5: сигналы профилей с ask выше потолка пишем (идея bot{N}hi), пока ask не выше этого
const BOT_ENTRY_PRICE = 0.97; // бот считает размер в акциях как «доллары / 0.97»
const DAY_MS = 24 * 60 * 60 * 1000;
const LIVE_LOG_MAX = 4000;

// Что торгует реальный бот СЕЙЧАС: идея -> размер ставки в $. Идея вида bot7b2 = профиль 7 + корзина Binance №2 (50-80%).
// Корзины: b0 = Binance против, b1 = меньше 50%, b2 = 50-80%, b3 = 80-120%, b4 = больше 120%. Без «b» = без фильтра.
// v3.1: то же для Coinbase: bot7c0 … bot7c4; по двум биржам сразу: bot7k2 (обе против), bot7k1 (одна против), bot7k0 (обе не против).
// Если включишь в боте профиль 6 — добавь сюда  bot6: 7  (или тот размер, что стоит в боте).
const LIVE_VARIANTS: Record<string, number> = { bot7b2: 5 };

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

// v3.2 / «Темка»: покупка в начале окна и продажа по тейк-профиту.
const TEMKA_TIMEFRAMES = [5, 15]; // минуты
const TEMKA_OPEN_MAX_SEC = 30; // рынок берём в игру, только если мы застали его не позже N секунд после открытия окна (иначе статистика грязная)
const TEMKA_MARKET_ASK_MIN = 0.3; // «по рынку» на открытии: ask должен быть в разумных пределах (не перекошенный стакан)
const TEMKA_MARKET_ASK_MAX = 0.7;
const TEMKA_LIMIT_LEVELS = [0.5, 0.48, 0.45, 0.4, 0.35]; // уровни лимитных покупок (ждут, пока ask опустится до уровня)
const TEMKA_LIMIT_WAITS: Record<number, number[]> = { 5: [60, 180], 15: [180, 540] }; // сколько секунд от открытия окна лимитка живёт
const TEMKA_TP_DELTAS = [0.02, 0.03, 0.04, 0.05, 0.07, 0.1]; // тейк-профит: +N от цены входа (при входе 0.50 это 0.52 / 0.53 / 0.54 / 0.55 / 0.57 / 0.60)
const TEMKA_MIN_SIZE = 5; // на лучшем уровне стакана должно быть минимум столько акций, чтобы вход/выход считались исполненными
const TEMKA_CONFIRM_COOLDOWN_MS = 1000; // не чаще 1 проверки живым стаканом на токен в секунду
const TEMKA_MIN_FILLS = 30; // минимум входов для общего топа и значков ✅/❌
const TEMKA_MIN_FILLS_COIN = 15; // минимум входов для топа по монете

// «Контртренд»: монета резко (за TEMKA_CT_LOOKBACK_SEC секунд) ушла ПРОТИВ стороны, токен этой стороны подешевел до уровня — покупаем его
// ради отскока. Рывок меряем в «сигмах»: движение за окно ÷ (типичное 5-мин движение × √(окно/300)), как z в остальном LAB.
const TEMKA_CT_LEVELS = [0.4, 0.3, 0.2];
const TEMKA_CT_ZS = [1.5, 2.5];
const TEMKA_CT_LOOKBACK_SEC = 30;
const TEMKA_CT_MIN_ELAPSED_SEC = 30; // не раньше, чем через N секунд после открытия окна
const TEMKA_CT_MIN_SEC_TO_CLOSE = 60; // и не позже, чем за N секунд до закрытия

interface TemkaMode {
  id: string;
  label: string;
  level?: number; // лимитка: уровень покупки
  waitSec?: number; // лимитка: сколько секунд от открытия ждём
  ctLevel?: number; // контртренд: потолок ask токена
  ctZ?: number; // контртренд: минимальный рывок монеты против стороны (в σ)
}
function temkaModesFor(tf: number): TemkaMode[] {
  const waits = TEMKA_LIMIT_WAITS[tf] ?? TEMKA_LIMIT_WAITS[5];
  return [
    { id: "m", label: "по рынку на открытии окна" },
    ...TEMKA_LIMIT_LEVELS.flatMap((l) =>
      waits.map((w) => ({ id: `l${l.toFixed(2)}w${w}`, label: `лимит ${l.toFixed(2)}, ждём ${w}с`, level: l, waitSec: w })),
    ),
    ...TEMKA_CT_LEVELS.flatMap((l) =>
      TEMKA_CT_ZS.map((z) => ({
        id: `ct${l.toFixed(2)}z${z.toFixed(1)}`,
        label: `контртренд: токен ≤ ${l.toFixed(2)}, рывок монеты против ≥ ${z.toFixed(1)}σ за ${TEMKA_CT_LOOKBACK_SEC}с`,
        ctLevel: l,
        ctZ: z,
      })),
    ),
  ];
}
const TEMKA_MODES_BY_TF: Record<number, TemkaMode[]> = Object.fromEntries(TEMKA_TIMEFRAMES.map((tf) => [tf, temkaModesFor(tf)]));
/** Ключ режима в агрегате: для 5м как раньше (совместимо со старой статистикой), для остальных с суффиксом таймфрейма. */
function temkaKeyMode(mode: string, tf: number): string {
  return tf === 5 ? mode : `${mode}@${tf}`;
}

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

// v3.1 / A. Binance-против на всей сетке модели: граница «чуть против» / «сильно против» (в долях потока бота).
// ratio = движение по Binance ÷ движение по потоку бота. ratio < 0 — Binance уже за ценой открытия (против).
const MODEL_BIN_DOSE_CUT = -0.25;

function zLabel(b: [number, number]): string {
  return `сила ${b[1] >= 99 ? `${b[0]}+` : `${b[0]}-${b[1]}`}`;
}
function secLabel(b: [number, number]): string {
  return b[0] === 0 ? `до ${b[1]}с` : `${b[0]}-${b[1]}с`;
}
function priceLabel(b: [number, number]): string {
  return `${b[0].toFixed(2)}-${b[1].toFixed(2)}`;
}

// 9. «Пила»
const SAW_LOOKBACK_SEC = 180;
const SAW_STEP_SEC = 5;
const SAW_BUCKETS = [0, 1, 2, 3]; // 3 = «3 и больше»

function sawLabel(n: number): string {
  return n >= 3 ? "3+" : String(n);
}

// 10. «Пара BTC+ETH»
const PAIR_COINS = ["Bitcoin", "Ethereum"]; // порядок важен: [0] = BTC, [1] = ETH
const PAIR_WINDOWS = [60, 90, 120, 180];
const PAIR_MOVES = [0.001, 0.0015, 0.002, 0.003]; // 0.10% / 0.15% / 0.20% / 0.30%
const PAIR_BANDS: [number, number][] = [
  [0.95, 0.97],
  [0.97, 0.98],
];
const PAIR_MIN_TRADES = 10;
const PAIR_PRUNE_MS = 10 * 60 * 1000;

// v3.1 / D. «Справедливая цена»: p = Φ(d / (σ·√t)); edge = p − ask.
const FAIR_SHORT = ["<0", "0-2%", "2-4%", "4-6%", "6%+"];
const FAIR_EDGE_LABELS = FAIR_SHORT.map((s) => `edge ${s}`);
function fairIdx(edge: number): number {
  return edge < 0 ? 0 : edge < 0.02 ? 1 : edge < 0.04 ? 2 : edge < 0.06 ? 3 : 4;
}
// σ считается из тиков фида (реализованная волатильность за последние look секунд, шаг step секунд).
const FAIR_VARIANTS = [
  { idea: "fair1", look: 60, step: 3, label: "σ за последние 60с" },
  { idea: "fair2", look: 120, step: 5, label: "σ за последние 120с" },
];
// Защита от «тихого фида»: σ не опускается ниже этой доли от обычной (часовой) секундной σ.
const FAIR_SIGMA_FLOOR = 0.3;

// v3.1 / C. Режим волатильности: σ за короткий период ÷ обычная σ (из VolatilityTracker, то же что в z).
const REGIME_SHORT_SEC = 120;
const REGIME_LONG_SEC = 300;
const LOSS_WINDOWS_MAX = 400;

// v3.1 / E. Заражение между монетами
const CONTAGION_PEAK = 0.95; // токен соседа был не ниже
const CONTAGION_DROP_TO = 0.85; // а сейчас ниже
const CONTAGION_SOFT_PEAK = 0.9; // мягкий вариант: был не ниже 0.90 и упал на 0.10+
const CONTAGION_SOFT_DROP = 0.1;

// v3.1 / F. Траектория токена
const TRAJ_LOOKBACK_SEC = 15;
const ASK_HIST_KEEP_MS = 40 * 1000;

// 11-14. Бинанс и Coinbase, признаки лосей, согласие профилей, стопы
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
const BINANCE_MATCH_MS = 4000;
const BINANCE_FRESH_MS = 5000;

const COINBASE_PRODUCTS: Record<string, string> = {
  Bitcoin: "BTC-USD",
  Ethereum: "ETH-USD",
  Solana: "SOL-USD",
  XRP: "XRP-USD",
  Dogecoin: "DOGE-USD",
};
const COINBASE_HOSTS = ["https://api.exchange.coinbase.com", "https://api.coinbase.com"];
const COINBASE_POLL_MS = 1000;

/** Общая часть опроса биржи по REST раз в N мс: история цен за 15 минут, поиск цены на момент времени. */
abstract class RestFeed {
  protected hist: Record<string, { t: number; p: number }[]> = {};
  protected hostIdx = 0;
  private failStreak = 0;
  private polling = false;
  okCount = 0;
  failCount = 0;

  constructor(
    private readonly pollMs: number,
    private readonly keepMs: number,
    private readonly freshMs: number,
    private readonly matchMs: number,
  ) {}

  protected abstract hostCount(): number;
  /** Возвращает цены по монетам (название монеты -> цена). Кидает ошибку, если ничего не получено. */
  protected abstract fetchPrices(hostIdx: number, signal: AbortSignal): Promise<Record<string, number>>;

  start(): void {
    void this.poll();
    setInterval(() => void this.poll(), this.pollMs);
  }

  private async poll(): Promise<void> {
    if (this.polling) return;
    this.polling = true;
    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), 3000);
    try {
      const prices = await this.fetchPrices(this.hostIdx, ctrl.signal);
      const t = Date.now();
      let got = 0;
      for (const [coin, p] of Object.entries(prices)) {
        if (!Number.isFinite(p) || p <= 0) continue;
        const h = this.hist[coin] ?? (this.hist[coin] = []);
        h.push({ t, p });
        const cutoff = t - this.keepMs;
        while (h.length > 0 && h[0].t < cutoff) h.shift();
        got++;
      }
      if (got === 0) throw new Error("пустой ответ");
      this.okCount++;
      this.failStreak = 0;
    } catch {
      this.failCount++;
      this.failStreak++;
      if (this.failStreak >= 5) {
        this.hostIdx = (this.hostIdx + 1) % this.hostCount();
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
    return Date.now() - last.t <= this.freshMs ? last.p : null;
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
      if (h[i].t < ts - this.matchMs) break;
    }
    return best !== null && bestD <= this.matchMs ? best.p : null;
  }
}

/** Цена монет с Binance: опрос REST раз в секунду, история за 15 минут. Без внешних библиотек. */
class BinanceFeed extends RestFeed {
  constructor() {
    super(BINANCE_POLL_MS, BINANCE_KEEP_MS, BINANCE_FRESH_MS, BINANCE_MATCH_MS);
  }
  protected hostCount(): number {
    return BINANCE_HOSTS.length;
  }
  protected async fetchPrices(hostIdx: number, signal: AbortSignal): Promise<Record<string, number>> {
    const symbols = Object.values(BINANCE_SYMBOLS);
    const url = `${BINANCE_HOSTS[hostIdx]}/api/v3/ticker/price?symbols=${encodeURIComponent(JSON.stringify(symbols))}`;
    const resp = await fetch(url, { signal });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const arr: any[] = await resp.json();
    const out: Record<string, number> = {};
    for (const row of arr) {
      const coin = Object.keys(BINANCE_SYMBOLS).find((c) => BINANCE_SYMBOLS[c] === row.symbol);
      const p = Number(row.price);
      if (coin && Number.isFinite(p) && p > 0) out[coin] = p;
    }
    return out;
  }
}

/** Цена монет с Coinbase (публичный REST, раз в секунду, по одному запросу на монету). Без ключей. */
class CoinbaseFeed extends RestFeed {
  constructor() {
    super(COINBASE_POLL_MS, BINANCE_KEEP_MS, BINANCE_FRESH_MS, BINANCE_MATCH_MS);
  }
  protected hostCount(): number {
    return COINBASE_HOSTS.length;
  }
  protected async fetchPrices(hostIdx: number, signal: AbortSignal): Promise<Record<string, number>> {
    const host = COINBASE_HOSTS[hostIdx];
    const headers = { "User-Agent": "research-lab/3.1", Accept: "application/json" };
    const coins = Object.keys(COINBASE_PRODUCTS);
    const results = await Promise.allSettled(
      coins.map(async (coin) => {
        const prod = COINBASE_PRODUCTS[coin];
        const url = hostIdx === 0 ? `${host}/products/${prod}/ticker` : `${host}/v2/prices/${prod}/spot`;
        const resp = await fetch(url, { signal, headers });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const j: any = await resp.json();
        const p = Number(hostIdx === 0 ? j.price : j?.data?.amount);
        if (!Number.isFinite(p) || p <= 0) throw new Error("bad price");
        return [coin, p] as [string, number];
      }),
    );
    const out: Record<string, number> = {};
    for (const r of results) if (r.status === "fulfilled") out[r.value[0]] = r.value[1];
    return out;
  }
}

const binance = new BinanceFeed();
const coinbase = new CoinbaseFeed();

// ───── v3.4: ещё биржи (Bybit, OKX, Kraken, Bitget) — те же корзины «прошла ли биржа за потоком», что у Binance/Coinbase ─────

/** Опрос биржи по одному запросу на монету (публичный REST, без ключей). */
abstract class PerCoinFeed extends RestFeed {
  protected abstract hosts(): string[];
  protected abstract coinsMap(): Record<string, string>;
  protected abstract urlFor(host: string, sym: string): string;
  protected abstract parse(j: any): number;
  protected hostCount(): number {
    return this.hosts().length;
  }
  protected async fetchPrices(hostIdx: number, signal: AbortSignal): Promise<Record<string, number>> {
    const host = this.hosts()[hostIdx];
    const map = this.coinsMap();
    const results = await Promise.allSettled(
      Object.keys(map).map(async (coin) => {
        const resp = await fetch(this.urlFor(host, map[coin]), { signal, headers: { "User-Agent": "research-lab/3.4", Accept: "application/json" } });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const p = this.parse(await resp.json());
        if (!Number.isFinite(p) || p <= 0) throw new Error("bad price");
        return [coin, p] as [string, number];
      }),
    );
    const out: Record<string, number> = {};
    for (const r of results) if (r.status === "fulfilled") out[r.value[0]] = r.value[1];
    return out;
  }
}

const USDT_SYMBOLS: Record<string, string> = { Bitcoin: "BTCUSDT", Ethereum: "ETHUSDT", Solana: "SOLUSDT", XRP: "XRPUSDT", Dogecoin: "DOGEUSDT" };

class BybitFeed extends PerCoinFeed {
  constructor() {
    super(1000, BINANCE_KEEP_MS, BINANCE_FRESH_MS, BINANCE_MATCH_MS);
  }
  protected hosts(): string[] {
    return ["https://api.bybit.com", "https://api.bytick.com"];
  }
  protected coinsMap(): Record<string, string> {
    return USDT_SYMBOLS;
  }
  protected urlFor(host: string, sym: string): string {
    return `${host}/v5/market/tickers?category=spot&symbol=${sym}`;
  }
  protected parse(j: any): number {
    return Number(j?.result?.list?.[0]?.lastPrice);
  }
}

class OkxFeed extends PerCoinFeed {
  constructor() {
    super(1000, BINANCE_KEEP_MS, BINANCE_FRESH_MS, BINANCE_MATCH_MS);
  }
  protected hosts(): string[] {
    return ["https://www.okx.com", "https://aws.okx.com"];
  }
  protected coinsMap(): Record<string, string> {
    return { Bitcoin: "BTC-USDT", Ethereum: "ETH-USDT", Solana: "SOL-USDT", XRP: "XRP-USDT", Dogecoin: "DOGE-USDT" };
  }
  protected urlFor(host: string, sym: string): string {
    return `${host}/api/v5/market/ticker?instId=${sym}`;
  }
  protected parse(j: any): number {
    return Number(j?.data?.[0]?.last);
  }
}

class BitgetFeed extends PerCoinFeed {
  constructor() {
    super(1000, BINANCE_KEEP_MS, BINANCE_FRESH_MS, BINANCE_MATCH_MS);
  }
  protected hosts(): string[] {
    return ["https://api.bitget.com"];
  }
  protected coinsMap(): Record<string, string> {
    return USDT_SYMBOLS;
  }
  protected urlFor(host: string, sym: string): string {
    return `${host}/api/v2/spot/market/tickers?symbol=${sym}`;
  }
  protected parse(j: any): number {
    return Number(j?.data?.[0]?.lastPr);
  }
}

/** Kraken: одним запросом все пары, раз в 2с (у публичного API Kraken строже лимиты). */
class KrakenFeed extends RestFeed {
  constructor() {
    super(2000, BINANCE_KEEP_MS, 8000, 6000);
  }
  protected hostCount(): number {
    return 1;
  }
  protected async fetchPrices(_hostIdx: number, signal: AbortSignal): Promise<Record<string, number>> {
    const resp = await fetch("https://api.kraken.com/0/public/Ticker?pair=XBTUSD,ETHUSD,SOLUSD,XRPUSD,XDGUSD", { signal, headers: { "User-Agent": "research-lab/3.4" } });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const j: any = await resp.json();
    const out: Record<string, number> = {};
    for (const [key, v] of Object.entries<any>(j?.result ?? {})) {
      const k = key.toUpperCase();
      const coin = k.includes("XBT") || k.startsWith("XXBT") ? "Bitcoin" : k.includes("ETH") ? "Ethereum" : k.includes("SOL") ? "Solana" : k.includes("XRP") ? "XRP" : k.includes("XDG") || k.includes("DOGE") ? "Dogecoin" : null;
      const p = Number(v?.c?.[0]);
      if (coin && Number.isFinite(p) && p > 0) out[coin] = p;
    }
    return out;
  }
}

const bybit = new BybitFeed();
const okx = new OkxFeed();
const kraken = new KrakenFeed();
const bitget = new BitgetFeed();
/** Дополнительные биржи: ключ в записи сделки -> подпись и фид */
const EXTRA_EXCHANGES: { key: string; label: string; feed: RestFeed }[] = [
  { key: "by", label: "Bybit", feed: bybit },
  { key: "ok", label: "OKX", feed: okx },
  { key: "kr", label: "Kraken", feed: kraken },
  { key: "bg", label: "Bitget", feed: bitget },
];

const BIN_LABELS = [
  "Binance против (уже за ценой открытия)",
  "Binance прошёл меньше 50% от потока",
  "50-80% от потока",
  "80-120% (примерно как поток)",
  "Binance дальше от открытия, чем поток (больше 120%)",
];
const CB_LABELS = BIN_LABELS.map((s) => s.replace("Binance", "Coinbase"));
const K_LABELS = ["обе биржи не против", "одна из двух бирж против", "обе биржи против"];
const RATIO_SHORT = ["против", "меньше 50%", "50-80%", "80-120%", "больше 120%"];
function binBucketIdx(r: number): number {
  return r < 0 ? 0 : r < 0.5 ? 1 : r < 0.8 ? 2 : r < 1.2 ? 3 : 4;
}

/** Расшифровка идеи вида bot7 / bot7b2 / bot7c0 / bot7k2. */
function variantTitle(idea: string): string {
  const m = idea.match(/^bot(\d+)(?:([bc])(\d)|k(\d))?$/);
  if (!m) return idea;
  let s = `Профиль ${m[1]}`;
  if (m[2] === "b") s += `, ${BIN_LABELS[Number(m[3])]}`;
  else if (m[2] === "c") s += `, ${CB_LABELS[Number(m[3])]}`;
  else if (m[4] !== undefined) s += `, ${K_LABELS[Number(m[4])]}`;
  else s += ", без фильтра бирж";
  return s;
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

/** Функция распределения стандартного нормального закона (Абрамовиц-Стеган 26.2.17, ошибка < 1e-7). */
function normCdf(x: number): number {
  const t = 1 / (1 + 0.2316419 * Math.abs(x));
  const d = 0.3989422804014327 * Math.exp((-x * x) / 2);
  const p = d * t * (0.31938153 + t * (-0.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
  return x > 0 ? 1 - p : p;
}

// корзины новых признаков
const REGIME_LABELS = ["до 0.7", "0.7-1.0", "1.0-1.5", "1.5-2.5", "больше 2.5"];
function regimeIdx(r: number): number {
  return r < 0.7 ? 0 : r < 1 ? 1 : r < 1.5 ? 2 : r < 2.5 ? 3 : 4;
}
const NEAR_LABELS = ["0", "1", "2+"];
const TRAJ_LABELS = [
  "резко падает (−0.05 и ниже)",
  "падает (−0.05…−0.015)",
  "стоит (±0.015)",
  "растёт (+0.015…+0.05)",
  "резко растёт (+0.05 и выше)",
];
function trajIdx(d: number): number {
  return d <= -0.05 ? 0 : d <= -0.015 ? 1 : d < 0.015 ? 2 : d < 0.05 ? 3 : 4;
}
const HOUR_LABELS = Array.from({ length: 24 }, (_, i) => String(i).padStart(2, "0"));

const F_REGIME2 = "режим волатильности: σ за 2 мин ÷ обычная σ";
const F_REGIME5 = "режим волатильности: σ за 5 мин ÷ обычная σ";
const F_CONTAGION = "соседние монеты с просевшим токеном (был ≥0.95, стал <0.85)";
const F_CONTAGION_SOFT = "соседние монеты: токен упал на 0.10+ от пика (пик ≥0.90)";
const F_TRAJ = "траектория токена за 15с (ask сейчас − ask 15с назад)";
const F_FAIR = "запас справедливой цены (p − ask, σ за 120с)";
const F_CB = "Coinbase к потоку бота (доля пройденного)";
const F_EXCH2 = "сколько бирж (Binance, Coinbase) против";
const F_HOUR = "час суток UTC (по одному)";

interface FeatureDef {
  name: string;
  labels: string[];
  bucket: (e: LabEntry) => string | null;
  perProfile?: boolean; // считать ещё и по каждому профилю отдельно (featProf)
  loss?: boolean; // показывать в «лаб лоси» (по умолчанию да)
}

const FEATURE_DEFS: FeatureDef[] = [
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
  // ── v3.1 ──
  {
    name: F_REGIME2,
    labels: REGIME_LABELS,
    perProfile: true,
    bucket: (e) => (e.rv2 == null ? null : REGIME_LABELS[regimeIdx(e.rv2)]),
  },
  {
    name: F_REGIME5,
    labels: REGIME_LABELS,
    perProfile: true,
    bucket: (e) => (e.rv5 == null ? null : REGIME_LABELS[regimeIdx(e.rv5)]),
  },
  {
    name: F_CONTAGION,
    labels: NEAR_LABELS,
    perProfile: true,
    bucket: (e) => (e.cg === undefined ? null : NEAR_LABELS[Math.min(e.cg, 2)]),
  },
  {
    name: F_CONTAGION_SOFT,
    labels: NEAR_LABELS,
    perProfile: true,
    bucket: (e) => (e.cgs === undefined ? null : NEAR_LABELS[Math.min(e.cgs, 2)]),
  },
  {
    name: F_TRAJ,
    labels: TRAJ_LABELS,
    perProfile: true,
    bucket: (e) => (e.tr15 == null ? null : TRAJ_LABELS[trajIdx(e.tr15)]),
  },
  {
    name: F_FAIR,
    labels: FAIR_SHORT,
    perProfile: true,
    bucket: (e) => (e.fe == null ? null : FAIR_SHORT[fairIdx(e.fe)]),
  },
  {
    name: F_CB,
    labels: RATIO_SHORT,
    perProfile: true,
    bucket: (e) => (e.cb == null || e.pm == null || e.pm <= 0 ? null : RATIO_SHORT[binBucketIdx(e.cb / e.pm)]),
  },
  {
    name: F_EXCH2,
    labels: ["0", "1", "2"],
    perProfile: true,
    bucket: (e) => {
      if (e.bin == null || e.cb == null) return null;
      return String((e.bin < 0 ? 1 : 0) + (e.cb < 0 ? 1 : 0));
    },
  },
  {
    name: F_HOUR,
    labels: HOUR_LABELS,
    loss: false,
    bucket: (e) => (e.hr === undefined ? null : HOUR_LABELS[e.hr]),
  },
];

const NUM_FEATURES: { name: string; get: (e: LabEntry) => number | null }[] = [
  { name: "сила движения z", get: (e) => e.z ?? null },
  { name: "скорость монеты за 10с, %", get: (e) => (e.sp10 == null ? null : e.sp10 * 100) },
  { name: "скорость монеты за 30с, %", get: (e) => (e.sp30 == null ? null : e.sp30 * 100) },
  { name: "движение от открытия по потоку бота, %", get: (e) => (e.pm == null ? null : e.pm * 100) },
  { name: "движение от открытия по Binance, %", get: (e) => (e.bin == null ? null : e.bin * 100) },
  { name: "движение от открытия по Coinbase, %", get: (e) => (e.cb == null ? null : e.cb * 100) },
  {
    name: "размер bid / размер ask",
    get: (e) => (e.bsz == null || e.asz == null || e.asz <= 0 ? null : Math.min(e.bsz / e.asz, 50)),
  },
  { name: "другие монеты идут туда же (0.05%)", get: (e) => e.c05 },
  { name: "пересечения цены открытия", get: (e) => e.xings ?? null },
  { name: "сколько профилей согласны", get: (e) => e.agreeN ?? null },
  { name: "режим: σ 2 мин ÷ обычная", get: (e) => e.rv2 ?? null },
  { name: "режим: σ 5 мин ÷ обычная", get: (e) => e.rv5 ?? null },
  { name: "соседей с просевшим токеном", get: (e) => e.cg ?? null },
  { name: "траектория токена за 15с", get: (e) => e.tr15 ?? null },
  { name: "запас справедливой цены (p − ask), %", get: (e) => (e.fe == null ? null : e.fe * 100) },
  { name: "секунд до закрытия", get: (e) => e.sec },
  { name: "цена входа", get: (e) => e.price },
];

/** Правила «что было бы, если не входить при такой корзине» для новых признаков (n = индекс корзины). */
const FEAT_RULES: Record<string, { label: string; remove: (n: number) => boolean }[]> = {
  [F_REGIME2]: [
    { label: "не входить при σ2м/обычная > 1.0", remove: (n) => n >= 2 },
    { label: "не входить при σ2м/обычная > 1.5", remove: (n) => n >= 3 },
    { label: "не входить при σ2м/обычная > 2.5", remove: (n) => n >= 4 },
  ],
  [F_REGIME5]: [
    { label: "не входить при σ5м/обычная > 1.0", remove: (n) => n >= 2 },
    { label: "не входить при σ5м/обычная > 1.5", remove: (n) => n >= 3 },
    { label: "не входить при σ5м/обычная > 2.5", remove: (n) => n >= 4 },
  ],
  [F_CONTAGION]: [
    { label: "не входить при 1+ просевших соседях", remove: (n) => n >= 1 },
    { label: "не входить при 2+ просевших соседях", remove: (n) => n >= 2 },
  ],
  [F_CONTAGION_SOFT]: [
    { label: "не входить при 1+ соседях с падением на 0.10+", remove: (n) => n >= 1 },
    { label: "не входить при 2+ соседях с падением на 0.10+", remove: (n) => n >= 2 },
  ],
  [F_TRAJ]: [
    { label: "не входить, если токен «падает» или «резко падает»", remove: (n) => n <= 1 },
    { label: "не входить, если токен «резко падает»", remove: (n) => n === 0 },
    { label: "не входить, если токен «растёт» или «резко растёт»", remove: (n) => n >= 3 },
  ],
  [F_FAIR]: [
    { label: "не входить при edge меньше 2%", remove: (n) => n <= 1 },
    { label: "не входить при edge меньше 4%", remove: (n) => n <= 2 },
  ],
  [F_CB]: [{ label: "не входить, если Coinbase против", remove: (n) => n === 0 }],
  [F_EXCH2]: [
    { label: "не входить, если хоть одна биржа против", remove: (n) => n >= 1 },
    { label: "не входить, если обе биржи против", remove: (n) => n >= 2 },
  ],
};

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

/** То же, но произвольное правило отсечения (remove(n) = true — корзину отбрасываем). */
function cutLine(buckets: Stat[], remove: (n: number) => boolean, label: string): string | null {
  const tot = buckets.reduce((a, s) => ({ w: a.w + s.w, t: a.t + s.t, roi: a.roi + s.roi }), { w: 0, t: 0, roi: 0 });
  if (tot.t === 0) return null;
  const baseRoi = tot.roi / tot.t;
  let kw = 0;
  let kt = 0;
  let kr = 0;
  let cw = 0;
  let ct = 0;
  buckets.forEach((s, n) => {
    if (remove(n)) {
      cw += s.w;
      ct += s.t;
    } else {
      kw += s.w;
      kt += s.t;
      kr += s.roi;
    }
  });
  if (ct === 0) return `  ${label}: ничего не отсекает`;
  const helps = kt > 0 && kr / kt > baseRoi && ct - cw > cw;
  return `  ${label}: остаётся ${kw}/${kt} (${winPct(kw, kt)}), прибыль ${roiLabel(kr, kt)}; отсекли ${ct} (побед ${cw}, проигрышей ${ct - cw}) ${helps ? "✅ помогает" : "— не помогает"}`;
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
    // Профиль 6 «Запас» — как в боте v7.7: z вместо порога, коридор 0.97-0.98, вход только при живом bid не ниже 0.97.
    id: 6,
    mode: "margin",
    priceMode: "corridor",
    requireLiveBid: true,
    windowSec: { Bitcoin: 60, Ethereum: 180, Solana: 60, XRP: 180, Dogecoin: 120 },
    param: { Bitcoin: 2.0, Ethereum: 4.0, Solana: 1.5, XRP: 2.5, Dogecoin: 1.0 },
  },
  {
    // Профиль 7 «Модель» — как в боте v7.9: z от 2.0, последние 180с, цена токена 0.93-0.96, живой bid не ниже 0.93.
    // Фильтр Binance в этой версии — отдельные «боты» по корзинам (bot7b0 … bot7b4), см. LIVE_VARIANTS.
    id: 7,
    mode: "margin",
    priceMode: "corridor",
    priceLow: 0.92, // v3.6: как в боте сейчас (было 0.93-0.96)
    priceHigh: 0.98,
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

/** Базовая запись профиля бота: bot1 … bot7 (без суффикса корзины Binance). */
function isBase(idea: string): boolean {
  return /^bot\d+$/.test(idea);
}

function fireKey(idea: string, combo: string, slug: string, side: string): string {
  return ONE_PER_MARKET ? `${idea}|${combo}|${slug}` : `${idea}|${combo}|${slug}|${side}`;
}
function pairKey(pk: string, closeTimeMs: number, side: string): string {
  return ONE_PER_MARKET ? `${pk}|${closeTimeMs}` : `${pk}|${closeTimeMs}|${side}`;
}

interface LabEntry {
  idea: string; // "margin" | "cheap" | "panic" | "impulse" | "model" | "modelb0".. | "fair1" | "fair2" | "psolo" | "bot1".."bot7" | "bot7b0".."bot7k2"
  combo: string;
  coin: string;
  side: "Up" | "Down";
  ts: number;
  price: number; // цена входа = живой ask
  sec: number; // секунд до закрытия в момент входа
  c05: number; // сколько других монет идут в ту же сторону >= 0.05%
  c10: number; // то же для >= 0.10%
  xings?: number; // сколько раз монета пересекла цену открытия за последние 3 мин (только базовые bot-входы)
  z?: number | null;
  sp10?: number | null;
  sp30?: number | null;
  hr?: number;
  bsz?: number | null;
  asz?: number | null;
  bin?: number | null;
  cb?: number | null; // движение по Coinbase от открытия окна (в нашу сторону)
  pm?: number;
  rv2?: number | null; // σ за 2 мин ÷ обычная σ
  rv5?: number | null; // σ за 5 мин ÷ обычная σ
  cg?: number; // соседей с просевшим токеном (строго)
  cgs?: number; // соседей с токеном, упавшим на 0.10+ от пика (мягко)
  tr15?: number | null; // ask сейчас − ask 15с назад
  fp?: number | null; // справедливая вероятность p
  fe?: number | null; // edge = p − ask
  xs?: Record<string, number | null>; // v3.4: Bybit/OKX/Kraken/Bitget
  agreeN?: number;
  bid0?: number | null; // v3.3: bid в момент входа
  pk?: number; // v3.3: пик цены токена до входа
  minBid?: number;
  stops?: (number | null)[];
}

interface Feat {
  z: number | null;
  sp10: number | null;
  sp30: number | null;
  hr: number;
  bsz: number | null;
  asz: number | null;
  bin: number | null;
  cb: number | null;
  pm: number;
  rv2: number | null;
  rv5: number | null;
  cg: number;
  cgs: number;
  tr15: number | null;
  fe: number | null;
  xs: Record<string, number | null>; // v3.4: движение остальных бирж в нашу сторону
}

interface StopAgg {
  n: number;
  w: number;
  stopped: number;
  stopW: number;
  stopL: number;
  roiNo: number;
  roiStop: number;
}

interface LiveRec {
  ts: number;
  idea: string;
  coin: string;
  side: string;
  price: number;
  won: boolean;
  pnl: number; // в долларах с размером ставки бота
  z: number | null;
  ratio: number | null; // во сколько раз Binance прошёл от потока
}

interface LossWin {
  lost: string[]; // монеты, где в этом окне проиграл хотя бы один базовый вход
  seen: string[]; // монеты, где в этом окне были базовые входы
  rvSum: number; // сумма rv2 по входам окна
  rvN: number;
}

interface PairEntry {
  pk: string; // `${окноBTC}:${движениеBTC}:${окноETH}:${движениеETH}:${индекс коридора}`
  side: "Up" | "Down";
  ts: number;
  closeTimeMs: number;
  btcSlug: string;
  ethSlug: string;
  btcPrice: number;
  ethPrice: number;
  sec: number;
}

interface LegState {
  sec: number;
  dir: number;
  price: number;
  ask: number;
}

interface ArbMarketInfo {
  minSum: number;
  sec99: number | null;
  size99: number | null;
}

/** Одна «ордер-позиция» идеи «Темка»: покупка (по рынку или лимиткой) и набор тейк-профитов. */
interface TemkaPos {
  mode: string; // см. TEMKA_MODES
  side: "Up" | "Down";
  level?: number; // для лимитки: цена, по которой ждём покупку
  deadlineMs?: number; // для лимитки: до какого момента ждём
  filled: boolean;
  price: number; // цена входа
  fillTs: number;
  hits: (number | null)[]; // по каждому TP: секунд от входа до достижения цели (null — не достигнута)
}

/** Агрегат «Темки»: ключ `${режим}|${h | индекс TP}|${монета | *}|${сторона | *}`. */
interface TStat {
  n: number; // ордеров поставлено (только в строке "h")
  f: number; // сколько вошло
  w: number; // из вошедших: выиграло при удержании до резолва (только "h")
  px: number; // сумма цен входа (только "h")
  roiH: number; // сумма доходностей «держим до резолва»
  roiH2: number; // сумма квадратов (для доверительного интервала)
  hit: number; // сколько раз достигнут TP (строки по TP)
  hitSec: number; // сумма секунд до TP
  roiT: number; // сумма доходностей «TP, иначе держим до резолва»
  roiT2: number;
  nhW: number; // не достигли TP, но выиграли на резолве
}

interface PendingMarket {
  closeTimeMs: number;
  coin: string;
  entries: LabEntry[];
  arb: ArbMarketInfo | null;
  temka?: TemkaPos[]; // v3.2
  temkaInit?: boolean; // v3.2: ордера «Темки» по этому рынку уже поставлены (или рынок пропущен)
  wm?: number; // v3.2: длина окна в минутах (нет поля — 5)
  tape?: TapeRow[]; // v3.3: лента цен раз в 5с
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
  wr: number;
  avgPx: number;
  lo: number;
  edge: number;
  roiAvg: number;
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

function usdLabel(v: number): string {
  return `${v >= 0 ? "+" : "-"}$${Math.abs(v).toFixed(2)}`;
}

/** Осторожная оценка винрейта: нижняя граница интервала Уилсона (95%). */
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
 * (но не раньше открытия окна). Замер раз в SAW_STEP_SEC секунд — число это нижняя оценка.
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

  private quotes = new Map<string, Quote>();
  private peaks = new Map<string, number>();
  private openPrices = new Map<string, number>();
  private touchStates = new Map<string, { touchedHigh: boolean; touchedCorridor: boolean; leftAfterTouch: boolean }>();
  private firedKeys = new Set<string>(); // см. fireKey(): комбинация входит в рынок один раз на сторону

  // v3.1: история ask токена (для траектории) и кэш реализованной волатильности
  private askHist = new Map<string, { t: number; a: number }[]>();
  private rvCache = new Map<string, { t: number; v: number | null }>();

  private verifying = new Set<string>();
  private lastVerifyAt = new Map<string, number>();
  private arbSampling = false;

  // v3.2 «Темка»
  private temkaAgg: Record<string, TStat> = {};
  private temkaOpening = new Set<string>(); // eventSlug: сейчас идёт постановка ордеров
  private temkaConfirming = new Set<string>(); // tokenId: сейчас идёт проверка живым стаканом
  private temkaLastConfirm = new Map<string, number>();

  private pending = new Map<string, PendingMarket>();
  private settled = new Set<string>();

  // ограничения «как у бота» — на каждую комбинацию отдельно
  private varWin = new Map<string, Map<number, number>>(); // `${idea}|${combo}` -> closeTimeMs -> сколько позиций в окне
  private varOpen = new Map<string, number>(); // `${idea}|${combo}` -> сколько позиций ждут резолва
  private liveLog: LiveRec[] = [];
  private liveSkips: Record<string, number> = {};
  private liveSkipSeen = new Set<string>();

  private agg: StatMap = {};
  private conf: StatMap = {};
  private agree: StatMap = {};
  private saw: StatMap = {};
  private arbAgg: Record<string, ArbAgg> = {};

  // «Пара BTC+ETH»
  private pairAgg: StatMap = {};
  private pairs: PairEntry[] = [];
  private pairFired = new Set<string>(); // см. pairKey()
  private pairVerifying = new Set<string>();
  private pairLastVerify = new Map<string, number>();
  private winners = new Map<string, "Up" | "Down">();

  // Binance-фильтры, признаки, согласие, стопы
  private binAgg: StatMap = {};
  private agAgg: StatMap = {};
  private featAgg: StatMap = {};
  private featProf: StatMap = {}; // v3.1: новые признаки по профилям: `${all|botN}|${признак}|${корзина}`
  private fairCal: StatMap = {}; // v3.1: калибровка справедливой цены: `${idea}|${edge-корзина}`, px = сумма p
  private lossWin: Record<string, LossWin> = {}; // v3.1: кластеры проигрышей по окнам (closeTimeMs)
  private featSum: Record<string, { s: number; n: number }> = {};
  private lossLog: LabEntry[] = [];
  private dipAgg: StatMap = {};
  private stopAgg: Record<string, StopAgg> = {};

  // счётчики честности цен
  private staleSampled = 0;
  private staleShifted = 0;
  private verifyConfirmed = 0;
  private verifyRejected = 0;

  private marketsSeenCount = 0;
  private resolvedCount = 0;
  private updateCount = 0;
  private sinceTs = Date.now();

  // ───── ограничения «как у бота» ─────

  private limitReason(idea: string, combo: string, closeTimeMs: number): string | null {
    const vk = `${idea}|${combo}`;
    if ((this.varWin.get(vk)?.get(closeTimeMs) ?? 0) >= MAX_PER_WINDOW) return "лимит окна";
    if ((this.varOpen.get(vk) ?? 0) >= MAX_OPEN_TOTAL) return "лимит всего";
    return null;
  }

  private takeSlot(idea: string, combo: string, closeTimeMs: number): void {
    const vk = `${idea}|${combo}`;
    let w = this.varWin.get(vk);
    if (!w) {
      w = new Map();
      this.varWin.set(vk, w);
    }
    w.set(closeTimeMs, (w.get(closeTimeMs) ?? 0) + 1);
    this.varOpen.set(vk, (this.varOpen.get(vk) ?? 0) + 1);
  }

  private releaseSlot(idea: string, combo: string): void {
    const vk = `${idea}|${combo}`;
    const n = (this.varOpen.get(vk) ?? 0) - 1;
    if (n > 0) this.varOpen.set(vk, n);
    else this.varOpen.delete(vk);
  }

  private pruneWindows(now: number): void {
    for (const [vk, w] of this.varWin) {
      for (const close of w.keys()) if (close < now - 10 * 60 * 1000) w.delete(close);
      if (w.size === 0) this.varWin.delete(vk);
    }
    if (this.liveSkipSeen.size > 3000) this.liveSkipSeen.clear();
  }

  /** Записывает, что реальный бот в этом месте пропустил бы вход (только для идей из LIVE_VARIANTS). */
  private noteLive(idea: string, slug: string, reason: string): void {
    if (LIVE_VARIANTS[idea] === undefined) return;
    const key = `${idea}|${slug}|${reason}`;
    if (this.liveSkipSeen.has(key)) return;
    this.liveSkipSeen.add(key);
    this.liveSkips[reason] = (this.liveSkips[reason] ?? 0) + 1;
  }

  // ───── история ask и реализованная волатильность (v3.1) ─────

  private pushAsk(tokenId: string, t: number, a: number): void {
    let h = this.askHist.get(tokenId);
    if (!h) {
      h = [];
      this.askHist.set(tokenId, h);
    }
    const last = h[h.length - 1];
    if (last && last.a === a && t - last.t < 1000) return;
    h.push({ t, a });
    const cutoff = t - ASK_HIST_KEEP_MS;
    // оставляем один замер старше границы, чтобы знать цену «15с назад», даже если ask давно не менялся
    while (h.length > 1 && h[1].t < cutoff) h.shift();
  }

  private askAgo(tokenId: string, now: number, sec: number): number | null {
    const h = this.askHist.get(tokenId);
    if (!h || h.length === 0) return null;
    const target = now - sec * 1000;
    for (let i = h.length - 1; i >= 0; i--) {
      if (h[i].t <= target) return h[i].a;
    }
    // истории не хватает: берём самый старый замер, если он покрывает хотя бы 60% нужного срока
    return now - h[0].t >= sec * 600 ? h[0].a : null;
  }

  /**
   * Реализованная секундная волатильность монеты (доля цены на √секунду) за последние lookSec секунд,
   * по тикам фида с шагом stepSec. Кэш на 1 секунду. null — если данных мало.
   */
  private realizedVol(coin: string, lookSec: number, stepSec: number, now: number): number | null {
    const key = `${coin}|${lookSec}|${stepSec}`;
    const c = this.rvCache.get(key);
    if (c && now - c.t < 1000) return c.v;
    const feed = PRICE_FEEDS[coin];
    let v: number | null = null;
    if (feed) {
      let prevP: number | null = null;
      let prevT = 0;
      let sumSq = 0;
      let sumDt = 0;
      let n = 0;
      const consider = (t: number, px: number | null): void => {
        if (px === null || px <= 0) return;
        if (prevP !== null && t > prevT) {
          const r = Math.log(px / prevP);
          sumSq += r * r;
          sumDt += (t - prevT) / 1000;
          n++;
        }
        prevP = px;
        prevT = t;
      };
      for (let t = now - lookSec * 1000; t < now; t += stepSec * 1000) consider(t, feed.getPriceAt(t));
      consider(now, feed.getLatestPrice());
      const need = Math.max(5, Math.floor((lookSec / stepSec) * 0.5));
      if (n >= need && sumDt > 0) v = Math.sqrt(sumSq / sumDt);
    }
    this.rvCache.set(key, { t: now, v });
    return v;
  }

  // ───── сохранение / загрузка ─────

  saveState(): void {
    try {
      const data = {
        version: STATE_VERSION,
        savedAt: Date.now(),
        sinceTs: this.sinceTs,
        agg: this.agg,
        conf: this.conf,
        agree: this.agree,
        saw: this.saw,
        pairAgg: this.pairAgg,
        binAgg: this.binAgg,
        agAgg: this.agAgg,
        featAgg: this.featAgg,
        featProf: this.featProf,
        fairCal: this.fairCal,
        lossWin: this.lossWin,
        featSum: this.featSum,
        lossLog: this.lossLog,
        dipAgg: this.dipAgg,
        stopAgg: this.stopAgg,
        liveLog: this.liveLog,
        liveSkips: this.liveSkips,
        pairs: this.pairs,
        winners: [...this.winners.entries()],
        arbAgg: this.arbAgg,
        temkaAgg: this.temkaAgg,
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
        const oldFile = path.resolve(process.cwd(), `research-lab-state.v${data.version}.json`);
        fs.renameSync(STATE_FILE, oldFile);
        console.log(
          `[loadState] найден файл состояния старой версии (v${data.version}): сохранён как research-lab-state.v${data.version}.json, статистика начинается с нуля.`,
        );
        return;
      }
      this.sinceTs = data.sinceTs ?? Date.now();
      this.agg = data.agg ?? {};
      this.conf = data.conf ?? {};
      this.agree = data.agree ?? {};
      this.saw = data.saw ?? {};
      this.pairAgg = data.pairAgg ?? {};
      this.binAgg = data.binAgg ?? {};
      this.agAgg = data.agAgg ?? {};
      this.featAgg = data.featAgg ?? {};
      this.featProf = data.featProf ?? {};
      this.fairCal = data.fairCal ?? {};
      this.lossWin = data.lossWin ?? {};
      this.featSum = data.featSum ?? {};
      this.lossLog = data.lossLog ?? [];
      this.dipAgg = data.dipAgg ?? {};
      this.stopAgg = data.stopAgg ?? {};
      this.liveLog = data.liveLog ?? [];
      this.liveSkips = data.liveSkips ?? {};
      this.pairs = data.pairs ?? [];
      this.winners = new Map(data.winners ?? []);
      for (const e of this.pairs) this.pairFired.add(pairKey(e.pk, e.closeTimeMs, e.side));
      this.arbAgg = data.arbAgg ?? {};
      this.temkaAgg = data.temkaAgg ?? {};
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
        for (const e of p.entries) {
          this.firedKeys.add(fireKey(e.idea, e.combo, slug, e.side));
          this.takeSlot(e.idea, e.combo, p.closeTimeMs);
        }
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
      p = { closeTimeMs: market.closeTimeMs, coin: market.coin, entries: [], arb: null, wm: market.windowMinutes };
      this.pending.set(market.eventSlug, p);
      if (market.windowMinutes === TARGET_WINDOW_MINUTES) this.marketsSeenCount++;
    }
    return p;
  }

  private dropPending(slug: string): void {
    const p = this.pending.get(slug);
    if (p) {
      for (const e of p.entries) {
        this.firedKeys.delete(fireKey(e.idea, e.combo, slug, e.side));
        this.releaseSlot(e.idea, e.combo);
      }
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
        (m.windowMinutes === TARGET_WINDOW_MINUTES || TEMKA_EXTRA_WINDOWS.includes(m.windowMinutes)) &&
        m.closeTimeMs - now <= observeWindowMs(m.windowMinutes),
    );

    this.tokenIndex = buildTokenIndex(markets);
    this.activeMarkets = new Map(markets.map((m) => [m.eventSlug, m]));
    const tokenIds = [...this.tokenIndex.keys()].sort();

    for (const m of markets) this.ensurePending(m);

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
    for (const tokenId of this.askHist.keys()) {
      if (!this.tokenIndex.has(tokenId)) this.askHist.delete(tokenId);
    }
    for (const tokenId of this.temkaLastConfirm.keys()) {
      if (!this.tokenIndex.has(tokenId)) this.temkaLastConfirm.delete(tokenId);
    }
    for (const tokenId of this.lastVerifyAt.keys()) {
      if (!this.tokenIndex.has(tokenId)) this.lastVerifyAt.delete(tokenId);
    }
    for (const [k, ts] of this.pairLastVerify) {
      if (now - ts > PAIR_PRUNE_MS) this.pairLastVerify.delete(k);
    }
    this.pruneWindows(now);
    for (const s of this.liveSlugs) if (!activeSlugs.has(s) && !this.pending.has(s) && !this.liveFills.has(s)) this.liveSlugs.delete(s);
    for (const k of this.liveWindowCount.keys()) if (k < now - 3600_000) this.liveWindowCount.delete(k);
    if (this.liveEnabled && this.clob) void this.clob.warmMeta(tokenIds).catch(() => {}); // метаданные рынков заранее — быстрый путь ордера

    console.log(
      `[refresh] наблюдаем рынков: ${markets.length} (5м ${markets.filter((m) => m.windowMinutes === TARGET_WINDOW_MINUTES).length}, 15м ${markets.filter((m) => m.windowMinutes === 15).length}; ${tokenIds.length} токенов), ждём резолва: ${this.pending.size}`,
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
    if ((hasBid || hasAsk) && q.ask !== null) this.pushAsk(update.tokenId, now, q.ask);

    // "цена" для условий коридора — как в боте: bestBid, иначе bestAsk
    const price = update.bestBid ?? update.bestAsk;
    if (price === null || price === undefined) return;

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

    // v3.2 «Темка»: постановка ордеров в начале окна, проверка исполнения лимиток и тейк-профитов
    this.temkaTick(market, side, p, q, now);

    // остальные идеи LAB подобраны под 5-минутки: 15-минутные рынки им не показываем
    if (market.windowMinutes !== TARGET_WINDOW_MINUTES) return;

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

  // ───── стоп, признаки, согласие ─────

  /** После входа запоминаем минимальный bid токена и первое касание каждого стоп-уровня (только базовые bot-входы). */
  private trackAfterEntry(side: "Up" | "Down", bid: number | null, p: PendingMarket, now: number): void {
    if (bid === null) return;
    for (const e of p.entries) {
      if (!isBase(e.idea) || e.side !== side || e.ts >= now) continue;
      if (e.minBid === undefined || bid < e.minBid) e.minBid = bid;
      if (!e.stops) e.stops = STOP_LEVELS.map(() => null);
      for (let i = 0; i < STOP_LEVELS.length; i++) {
        if (e.stops[i] === null && bid <= STOP_LEVELS[i]) e.stops[i] = bid;
      }
    }
  }

  private aggregateBotEntry(e: LabEntry, won: boolean, roi: number): void {
    if (e.agreeN === undefined) return;

    // Binance против потока бота (по этому входу без фильтра; настоящие «боты по корзинам» считаются отдельно)
    if (e.bin !== null && e.bin !== undefined && e.pm !== undefined && e.pm > 0) {
      const bi = binBucketIdx(e.bin / e.pm);
      bump(this.binAgg, `${e.idea}|b${bi}`, won, roi, e.price);
      bump(this.binAgg, `all|b${bi}`, won, roi, e.price);
    }

    const n = Math.min(Math.max(e.agreeN, 1), 3);
    bump(this.agAgg, `${e.idea}|n${n}`, won, roi, e.price);
    bump(this.agAgg, `all|n${n}`, won, roi, e.price);

    for (const def of FEATURE_DEFS) {
      const b = def.bucket(e);
      if (b === null) continue;
      bump(this.featAgg, `${def.name}|${b}`, won, roi, e.price);
      if (def.perProfile) {
        bump(this.featProf, `all|${def.name}|${b}`, won, roi, e.price);
        bump(this.featProf, `${e.idea}|${def.name}|${b}`, won, roi, e.price);
      }
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

  /** Сумма статистики идеи по всем комбинациям и монетам. */
  private sumIdea(idea: string): { w: number; t: number; roi: number; px: number } {
    let w = 0;
    let t = 0;
    let roi = 0;
    let px = 0;
    for (const s of this.collect(idea)) {
      w += s.w;
      t += s.t;
      roi += s.roi;
      px += s.px;
    }
    return { w, t, roi, px };
  }

  private verdictFor(w: number, t: number, px: number): { avg: number; verdict: "✅" | "⚠️" | "❌" } {
    const avg = t > 0 ? px / t : 0;
    const lo = wilsonLower(w, t);
    return { avg, verdict: lo > avg ? "✅" : t > 0 && w / t > avg ? "⚠️" : "❌" };
  }

  /** Одна строка «бот»: итог по идее вида botNbX / botNcX / botNkX. */
  private variantLine(idea: string, label: string): string {
    const s = this.sumIdea(idea);
    if (s.t === 0) return `  ${label}: пока нет входов`;
    const v = this.verdictFor(s.w, s.t, s.px);
    const live = LIVE_VARIANTS[idea] !== undefined ? " ← так торгует бот" : "";
    return `  ${label}: ${s.w}/${s.t} (${winPct(s.w, s.t)}), надёжно ${(wilsonLower(s.w, s.t) * 100).toFixed(0)}%, цена ${v.avg.toFixed(3)}, прибыль ${roiLabel(s.roi, s.t)} ${v.verdict}${s.t < 30 ? " (мало данных)" : ""}${live}`;
  }

  buildBinanceReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Второе мнение: Binance (каждая корзина — отдельный бот)</b>");
    lines.push(
      "Для каждого профиля бота считаются отдельные «боты»: без фильтра и с фильтром по корзине Binance. Фильтр работает как в реальном боте: условие входа, а не метка. Если в первый момент Binance не попал в корзину, ждём и входим позже, как бот. Правила: " +
        `${RULES_TEXT}.`,
    );
    lines.push(`Опрос Binance: удачно ${binance.okCount}, ошибок ${binance.failCount}. Данные считаются с ${new Date(this.sinceTs).toISOString().slice(0, 16).replace("T", " ")} UTC.`);
    lines.push("Прибыль — средний результат на $1 ставки (без комиссий). «Надёжно» — осторожная оценка винрейта; ✅ надёжно выше цены входа, ⚠️ винрейт выше цены, но данных мало, ❌ ниже.");
    lines.push("");
    let anyProfile = false;
    for (const prof of BOT_PROFILES) {
      const ideas: [string, string][] = [[`bot${prof.id}`, "без фильтра Binance"]];
      BIN_LABELS.forEach((label, i) => ideas.push([`bot${prof.id}b${i}`, label]));
      if (this.sumIdea(ideas[0][0]).t === 0) continue;
      anyProfile = true;
      lines.push(`<b>── Профиль ${prof.id} ──</b>`);
      for (const [idea, label] of ideas) lines.push(this.variantLine(idea, label));
      lines.push("");
    }
    if (!anyProfile) lines.push("пока нет данных");
    return lines.join("\n");
  }

  /** v3.1 / B: Coinbase и «две биржи сразу» для профилей бота. */
  buildExchangesReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Другие биржи: Coinbase и «две биржи сразу»</b>");
    lines.push(
      "Те же «боты по корзинам», что и для Binance, но по Coinbase (c0-c4), и по двум биржам сразу: k2 = обе против, k1 = одна против, k0 = обе не против. Корзина = во сколько раз биржа прошла от цены открытия окна по сравнению с потоком бота (против = биржа уже за ценой открытия). Фильтр — условие входа с ожиданием, как в боте.",
    );
    lines.push(
      `Опрос Coinbase: удачно ${coinbase.okCount}, ошибок ${coinbase.failCount}. Опрос Binance: удачно ${binance.okCount}, ошибок ${binance.failCount}. Данные считаются с ${new Date(this.sinceTs).toISOString().slice(0, 16).replace("T", " ")} UTC.`,
    );
    lines.push(
      "Важно: сначала проверь правила рынка Polymarket — по какому источнику идёт резолв (для 5-минуток это обычно Chainlink Data Streams, он агрегирует несколько площадок). Если вся логика в том, что токен тянется за биржами, а резолв идёт по агрегату, то совпадение нескольких бирж должно давать фильтр жёстче одной.",
    );
    lines.push("");
    let any = false;
    for (const prof of BOT_PROFILES) {
      if (this.sumIdea(`bot${prof.id}`).t === 0) continue;
      any = true;
      lines.push(`<b>── Профиль ${prof.id} ──</b>`);
      lines.push(this.variantLine(`bot${prof.id}`, "без фильтра бирж"));
      lines.push(this.variantLine(`bot${prof.id}b0`, "для сравнения: Binance против"));
      CB_LABELS.forEach((label, i) => lines.push(this.variantLine(`bot${prof.id}c${i}`, label)));
      K_LABELS.forEach((label, i) => lines.push(this.variantLine(`bot${prof.id}k${i}`, label)));
      lines.push("");
    }
    if (!any) lines.push("пока нет данных");
    lines.push(`Отдельно по признаку «${F_EXCH2}» и «${F_CB}» см. «лаб лоси» (там же числовые средние).`);
    return lines.join("\n");
  }

  buildAgreeReport(): string {
    const get = (key: string): Stat => this.agAgg[key] ?? { w: 0, t: 0, roi: 0 };
    const lines: string[] = [];
    lines.push("<b>📊 Согласие профилей в момент входа</b>");
    lines.push(
      "Сколько профилей бота дали сигнал одновременно в тот момент, когда сработал этот вход (считаются базовые входы профилей без фильтра Binance). Идея: если сигнал даёт сразу несколько профилей, вход надёжнее.",
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
      `К каждому базовому входу профилей бота (без фильтра Binance) записываем признаки момента входа и сравниваем проигравшие входы с выигравшими. Входов с признаками: ${totT}, из них проиграло ${totT - totW}. Новые признаки (режим волатильности, соседи, траектория, edge, Coinbase) накапливаются только с момента обновления до v3.1.`,
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
      if (def.loss === false) continue;
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
          `  ${t} UTC ${COIN_SHORT[e.coin] ?? e.coin} ${e.side}, профиль ${e.idea.slice(3)}, цена ${e.price.toFixed(2)}, за ${e.sec}с | z ${e.z != null ? e.z.toFixed(1) : "—"} | скорость 10с ${fmtPct(e.sp10)}, 30с ${fmtPct(e.sp30)} | bid/ask ${e.bsz != null ? Math.round(e.bsz) : "—"}/${e.asz != null ? Math.round(e.asz) : "—"} | Binance ${fmtPct(e.bin)} Coinbase ${fmtPct(e.cb)} (поток ${fmtPct(e.pm)}) | согласны профилей ${e.agreeN ?? "—"} | пересечений ${e.xings ?? "—"} | мин. цена после входа ${e.minBid != null ? e.minBid.toFixed(2) : "—"} | σ2м/обычная ${e.rv2 != null ? e.rv2.toFixed(1) : "—"} | соседей просело ${e.cg ?? "—"} | траектория 15с ${e.tr15 != null ? e.tr15.toFixed(2) : "—"} | edge ${fmtPct(e.fe, 1)}`,
        );
      }
    }
    return lines.join("\n");
  }

  buildStopReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Стоп-лосс: как глубоко проседает цена после входа</b>");
    lines.push(
      "Для каждого базового входа профилей бота пишем минимальную цену токена (bid) после входа до закрытия окна. Потом считаем, что было бы, если бы при падении цены до уровня мы продавали: сколько выигрышных входов стоп выбил бы зря, сколько проигрышных поймал, и что с прибылью.",
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

  /** «лаб бот» — как торговал бы реальный бот (идеи из LIVE_VARIANTS). */
  buildLiveReport(): string {
    const now = Date.now();
    const lines: string[] = [];
    lines.push("<b>🤖 Как торговал бы реальный бот</b>");
    lines.push(
      `Считается так: ${RULES_TEXT}; фильтр бирж с ожиданием, проверка живого стакана, цена входа = живой ask, итог по официальному резолву. Размер ставки как у бота. Счёт с ${new Date(this.sinceTs).toISOString().slice(0, 16).replace("T", " ")} UTC.`,
    );
    lines.push("");
    for (const idea of Object.keys(LIVE_VARIANTS)) {
      const size = LIVE_VARIANTS[idea];
      const m = idea.match(/^bot(\d+)/);
      const title = variantTitle(idea);
      const s = this.sumIdea(idea);
      const log = this.liveLog.filter((r) => r.idea === idea);
      lines.push(`<b>── ${title} (ставка $${size}) ──</b>`);
      if (s.t === 0) {
        lines.push("  пока нет сделок");
        lines.push("");
        continue;
      }
      const v = this.verdictFor(s.w, s.t, s.px);
      const pnlAll = log.reduce((a, r) => a + r.pnl, 0);
      lines.push(`  Итого: ${s.w}/${s.t} (${winPct(s.w, s.t)}), проигрышей ${s.t - s.w}, прибыль на $1: ${roiLabel(s.roi, s.t)}, в долларах ${usdLabel(pnlAll)}`);
      lines.push(`  Средняя цена входа ${v.avg.toFixed(3)} → безубыточный винрейт ${(v.avg * 100).toFixed(1)}%, фактический ${winPct(s.w, s.t)}, надёжная оценка ${(wilsonLower(s.w, s.t) * 100).toFixed(1)}% ${v.verdict}`);
      for (const [label, ms] of [["за 24 часа", DAY_MS], ["за 7 дней", 7 * DAY_MS]] as [string, number][]) {
        const part = log.filter((r) => r.ts >= now - ms);
        const w = part.filter((r) => r.won).length;
        const pnl = part.reduce((a, r) => a + r.pnl, 0);
        lines.push(`  ${label}: ${part.length > 0 ? `${w}/${part.length} (${winPct(w, part.length)}), проигрышей ${part.length - w}, ${usdLabel(pnl)}` : "сделок нет"}`);
      }
      const byCoin = this.collect(idea);
      const coinParts = INCLUDED_COINS.map((c) => {
        const x = byCoin.find((r) => r.coin === c);
        return x ? `${COIN_SHORT[c]} ${x.w}/${x.t} ${winPct(x.w, x.t)}` : null;
      }).filter((x) => x !== null);
      lines.push(`  По монетам: ${coinParts.join(" | ")}`);
      if (m && idea !== `bot${m[1]}`) {
        const base = this.sumIdea(`bot${m[1]}`);
        if (base.t > 0) {
          lines.push(`  Для сравнения тот же профиль без фильтра бирж: ${base.w}/${base.t} (${winPct(base.w, base.t)}), прибыль на $1: ${roiLabel(base.roi, base.t)}`);
        }
      }
      lines.push("");
    }

    lines.push("<b>── Что бот пропустил бы (уникальных рынков) ──</b>");
    const skipKeys = Object.keys(this.liveSkips);
    if (skipKeys.length === 0) lines.push("  пока ничего");
    for (const k of skipKeys) lines.push(`  ${k}: ${this.liveSkips[k]}`);
    lines.push("");

    lines.push("<b>── Последние проигрыши бота (до 10) ──</b>");
    const losses = this.liveLog.filter((r) => !r.won).slice(-10).reverse();
    if (losses.length === 0) {
      lines.push("  пока нет проигрышей");
    } else {
      for (const r of losses) {
        const t = new Date(r.ts).toISOString().slice(5, 16).replace("T", " ");
        lines.push(
          `  ${t} UTC ${COIN_SHORT[r.coin] ?? r.coin} ${r.side}, ${r.idea}, цена ${r.price.toFixed(3)}, z ${r.z !== null ? r.z.toFixed(1) : "—"}, Binance ${r.ratio !== null ? `${(r.ratio * 100).toFixed(0)}%` : "—"}, ${usdLabel(r.pnl)}`,
        );
      }
    }
    lines.push("");
    lines.push("Прибыль на $1 — средний результат на $1 ставки; в долларах — с реальным размером ставки бота (без комиссий).");
    return lines.join("\n");
  }

  // ───── «Пара BTC+ETH» ─────

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
      if (m.coin === otherCoin && m.closeTimeMs === market.closeTimeMs && m.windowMinutes === market.windowMinutes) {
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

    const fresh = this.pairHolds(lb, le).some((pk) => !this.pairFired.has(pairKey(pk, btcM.closeTimeMs, side)));
    if (!fresh) return;

    void this.verifyAndCommitPair(btcM, ethM, side, key);
  }

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
        const fk = pairKey(pk, btcM.closeTimeMs, side);
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
        const sk = `${e.closeTimeMs}:${e.side}`;
        if (!seenSituations.has(sk)) {
          seenSituations.add(sk);
          bump(this.pairAgg, "_sit|pair", wonB && wonE, 0);
          bump(this.pairAgg, "_sit|btc", wonB, 0);
          bump(this.pairAgg, "_sit|eth", wonE, 0);
        }
        this.pairs.splice(i, 1);
        this.pairFired.delete(pairKey(e.pk, e.closeTimeMs, e.side));
      } else if (now - e.closeTimeMs > RESOLVE_GIVE_UP_MS + PAIR_PRUNE_MS) {
        this.pairs.splice(i, 1);
        this.pairFired.delete(pairKey(e.pk, e.closeTimeMs, e.side));
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
      if (!book) return;
      this.quotes.set(tokenId, { bid: book.bid, ask: book.ask });
      if (book.ask !== null) this.pushAsk(tokenId, Date.now(), book.ask);

      if (this.pending.get(market.eventSlug) !== p) return;

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
   * v3.0: каждая комбинация записывает каждый сигнал (лимиты и «один вход на рынок» включаются константами вверху файла).
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
    const dirMove = side === "Up" ? pctMove : -pctMove;
    const peak = this.peaks.get(`${market.eventSlug}:${side}`) ?? 0;
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;

    let volCache: number | null | undefined;
    const getVol = (): number | null => {
      if (volCache === undefined) volCache = volTracker.getRecentVolatility(market.coin, now);
      return volCache ?? null;
    };
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
    let xingsCache: number | undefined;
    const getXings = (): number => {
      if (xingsCache === undefined) xingsCache = countCrossings(feed, openP, openTimeMs, now, coinNow);
      return xingsCache;
    };

    // Движение монеты по бирже от открытия окна в нашу сторону (Binance / Coinbase)
    const exMove = (ex: RestFeed): number | null => {
      const o = ex.getPriceAt(market.coin, openTimeMs);
      const n = ex.getLatest(market.coin);
      if (o === null || n === null || o === 0) return null;
      const m = (n - o) / o;
      return side === "Up" ? m : -m;
    };
    let binCache: number | null | undefined;
    const getBin = (): number | null => {
      if (binCache === undefined) binCache = exMove(binance);
      return binCache;
    };
    let cbCache: number | null | undefined;
    const getCb = (): number | null => {
      if (cbCache === undefined) cbCache = exMove(coinbase);
      return cbCache;
    };
    // Во сколько раз биржа прошла от потока бота (как в боте: нужно движение по потоку в нашу сторону и свежая биржа)
    const getRatioOf = (src: "b" | "c"): number | null => {
      const mv = src === "b" ? getBin() : getCb();
      return mv !== null && dirMove > 0 ? mv / dirMove : null;
    };
    const getRatio = (): number | null => getRatioOf("b");

    // Режим волатильности: σ за короткий период ÷ обычная секундная σ (та же, что в z)
    let regCache: { rv2: number | null; rv5: number | null } | undefined;
    const getRegime = (): { rv2: number | null; rv5: number | null } => {
      if (regCache === undefined) {
        const v = getVol();
        const base = v !== null && v > 1e-6 ? v / Math.sqrt(300) : null;
        const s2 = this.realizedVol(market.coin, REGIME_SHORT_SEC, 5, now);
        const s5 = this.realizedVol(market.coin, REGIME_LONG_SEC, 10, now);
        regCache = {
          rv2: base !== null && s2 !== null ? s2 / base : null,
          rv5: base !== null && s5 !== null ? s5 / base : null,
        };
      }
      return regCache;
    };

    // «Справедливая цена»: p = Φ(d / (σ·√t))
    const fairCache = new Map<string, { p: number; edge: number } | null>();
    const getFair = (look: number, step: number): { p: number; edge: number } | null => {
      const ck = `${look}:${step}`;
      if (fairCache.has(ck)) return fairCache.get(ck) ?? null;
      let res: { p: number; edge: number } | null = null;
      if (dirMove > 0) {
        let sig = this.realizedVol(market.coin, look, step, now);
        const v = getVol();
        const base = v !== null && v > 1e-6 ? v / Math.sqrt(300) : null;
        if (sig !== null && base !== null) sig = Math.max(sig, FAIR_SIGMA_FLOOR * base);
        if (sig !== null && sig > 0) {
          const pFair = normCdf(dirMove / (sig * Math.sqrt(Math.max(secToClose, 1))));
          res = { p: pFair, edge: pFair - ask };
        }
      }
      fairCache.set(ck, res);
      return res;
    };

    // «Заражение»: сколько соседних монет (то же окно закрытия) имеют просевший токен нашей стороны
    let contCache: { strict: number; soft: number } | undefined;
    const getContagion = (): { strict: number; soft: number } => {
      if (contCache === undefined) {
        let strict = 0;
        let soft = 0;
        for (const m of this.activeMarkets.values()) {
          if (m.closeTimeMs !== market.closeTimeMs || m.coin === market.coin || m.windowMinutes !== market.windowMinutes) continue;
          const tok = side === "Up" ? m.upTokenId : m.downTokenId;
          const pk = this.peaks.get(`${m.eventSlug}:${side}`) ?? 0;
          const q2 = this.quotes.get(tok);
          const cur = q2 ? (q2.bid ?? q2.ask) : null;
          if (cur === null) continue;
          if (pk >= CONTAGION_PEAK && cur < CONTAGION_DROP_TO) strict++;
          if (pk >= CONTAGION_SOFT_PEAK && pk - cur >= CONTAGION_SOFT_DROP) soft++;
        }
        contCache = { strict, soft };
      }
      return contCache;
    };

    // «Траектория токена»: ask сейчас − ask 15с назад
    const getTraj = (): number | null => {
      const a0 = this.askAgo(tokenId, now, TRAJ_LOOKBACK_SEC);
      return a0 === null ? null : ask - a0;
    };

    let featCache: Feat | undefined;
    const getFeat = (): Feat => {
      if (featCache === undefined) {
        const sp = (lb: number): number | null => {
          const p0 = feed.getPriceAt(now - lb * 1000);
          if (p0 === null || p0 === 0) return null;
          const m = (coinNow - p0) / p0;
          return side === "Up" ? m : -m;
        };
        const reg = getRegime();
        const cont = getContagion();
        featCache = {
          z: getZ(),
          sp10: sp(10),
          sp30: sp(30),
          hr: new Date(now).getUTCHours(),
          bsz: book ? book.bidSize : null,
          asz: book ? book.askSize : null,
          bin: getBin(),
          cb: getCb(),
          pm: dirMove,
          rv2: reg.rv2,
          rv5: reg.rv5,
          cg: cont.strict,
          cgs: cont.soft,
          tr15: getTraj(),
          fe: getFair(120, 5)?.edge ?? null,
          xs: Object.fromEntries(EXTRA_EXCHANGES.map((x) => [x.key, exMove(x.feed)])),
        };
      }
      return featCache;
    };
    let agreeNow = 0;

    let count = 0;
    const emit = (idea: string, combo: string, extra?: Partial<LabEntry>): void => {
      const key = fireKey(idea, combo, market.eventSlug, side);
      if (this.firedKeys.has(key)) return; // комбинация уже вошла в этот рынок
      const lim = this.limitReason(idea, combo, market.closeTimeMs);
      if (lim !== null) {
        if (commit) this.noteLive(idea, market.eventSlug, lim);
        return;
      }
      count++;
      if (!commit) return;
      this.firedKeys.add(key);
      this.takeSlot(idea, combo, market.closeTimeMs);
      const c = getConf();
      const entryRec: LabEntry = {
        idea,
        combo,
        coin: market.coin,
        side,
        ts: now,
        price: ask,
        sec: Math.round(secToClose),
        c05: c.c05,
        c10: c.c10,
        xings: isBase(idea) ? getXings() : undefined,
        ...getFeat(),
        bid0: bid,
        pk: peak,
        ...(isBase(idea) ? { agreeN: agreeNow } : {}),
        ...(extra ?? {}),
      };
      p.entries.push(entryRec);
      if (PAPER_TRADE[idea] !== undefined) {
        if (this.liveEnabled && this.clob) void this.liveBuy(idea, market, side, entryRec, getRatioOf("b"));
        else this.paperBuy(idea, market, side, entryRec, getRatioOf("b"));
      }
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
    // v3.1: те же ячейки дублируются по корзинам Binance (modelb0..b4, «против» делится на чуть/сильно: modelbW / modelbS),
    // по корзинам Coinbase (modelc0..c4) и по двум биржам сразу (modelk0..k2).
    if (secToClose <= MODEL_MAX_SEC && ask >= MODEL_ASK_MIN && ask < MODEL_ASK_MAX) {
      const z = getZ();
      if (z !== null) {
        const zb = findBand(MODEL_Z_BANDS, z);
        const sb = findBand(MODEL_SEC_BANDS, secToClose);
        const pb = findBand(MODEL_PRICE_BANDS, ask);
        if (zb && sb && pb) {
          const combo = `${zLabel(zb)} / ${secLabel(sb)} / ${priceLabel(pb)}`;
          emit("model", combo);
          const rb = getRatioOf("b");
          const rc = getRatioOf("c");
          if (rb !== null) {
            emit(`modelb${binBucketIdx(rb)}`, combo);
            if (rb < 0) emit(rb >= MODEL_BIN_DOSE_CUT ? "modelbW" : "modelbS", combo);
          }
          if (rc !== null) emit(`modelc${binBucketIdx(rc)}`, combo);
          if (rb !== null && rc !== null) emit(`modelk${(rb < 0 ? 1 : 0) + (rc < 0 ? 1 : 0)}`, combo);
        }
      }
    }

    // ── 15. «Справедливая цена»: p = Φ(d / (σ·√t)), edge = p − ask. Ячейки: корзина edge × секунды × цена. ──
    if (secToClose <= MODEL_MAX_SEC && dirMove > 0 && ask >= MODEL_ASK_MIN && ask < MODEL_ASK_MAX) {
      const sb = findBand(MODEL_SEC_BANDS, secToClose);
      const pb = findBand(MODEL_PRICE_BANDS, ask);
      if (sb && pb) {
        for (const fv of FAIR_VARIANTS) {
          const f = getFair(fv.look, fv.step);
          if (!f) continue;
          const combo = `${FAIR_EDGE_LABELS[fairIdx(f.edge)]} / ${secLabel(sb)} / ${priceLabel(pb)}`;
          const extra: Partial<LabEntry> = { fp: f.p, fe: f.edge };
          emit(fv.idea, combo, extra);
          if (fv.idea === "fair2") {
            const rb = getRatioOf("b");
            if (rb !== null && rb < 0) emit("fair2b0", combo, extra);
          }
        }
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

    // ── 7. Профили бота: базовый бот (без фильтра) и боты по корзинам Binance / Coinbase / двух бирж ──
    /** Подходит ли идея из LIVE_VARIANTS (бот этого профиля с его фильтром бирж) под текущее состояние бирж. */
    const variantMatches = (idea: string, id: number): boolean => {
      const m = idea.match(/^bot(\d+)(?:([bc])(\d)|k(\d))?$/);
      if (!m || Number(m[1]) !== id) return false;
      if (m[2] === undefined && m[4] === undefined) return true;
      if (m[2] !== undefined) {
        const r = getRatioOf(m[2] as "b" | "c");
        return r !== null && binBucketIdx(r) === Number(m[3]);
      }
      const rb = getRatioOf("b");
      const rc = getRatioOf("c");
      if (rb === null || rc === null) return false;
      return (rb < 0 ? 1 : 0) + (rc < 0 ? 1 : 0) === Number(m[4]);
    };
    /** Бот из LIVE_VARIANTS пропустил бы вход по этой причине (если его фильтр бирж сейчас тоже пропускает). */
    const noteLiveFail = (id: number, reason: string): void => {
      if (!commit) return;
      for (const idea of Object.keys(LIVE_VARIANTS)) {
        if (variantMatches(idea, id)) this.noteLive(idea, market.eventSlug, reason);
      }
    };

    const holding: number[] = [];
    const aboveCap: number[] = [];
    for (const prof of BOT_PROFILES) {
      const win = prof.windowSec[market.coin];
      if (win === undefined || secToClose > win) continue;

      const low = prof.priceLow ?? DEFAULT_LOW;
      const high = prof.priceHigh ?? DEFAULT_HIGH;

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

      if (prof.mode === "margin") {
        const z = getZ();
        if (z === null || z < pv) continue;
      } else {
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
      }

      // сигнал профиля есть — дальше как перед покупкой в боте: потолок цены и живой bid
      if (ask > high) {
        noteLiveFail(prof.id, "покупка не прошла: ask выше потолка коридора");
        // v3.5: записываем и такие сигналы отдельной идеей bot{N}hi — «а что было бы, если бы потолок был выше»
        if (ask <= ABOVE_CAP_MAX_ASK && !(prof.requireLiveBid && (bid === null || bid < low - 1e-9))) aboveCap.push(prof.id);
        continue;
      }
      if (prof.requireLiveBid && (bid === null || bid < low - 1e-9)) {
        noteLiveFail(prof.id, "покупка не прошла: живой bid ниже коридора");
        continue;
      }
      holding.push(prof.id);
    }
    agreeNow = holding.length;
    for (const id of aboveCap) {
      const lo = ask <= 0.97 ? 0.96 : ask <= 0.98 ? 0.97 : ask <= 0.99 ? 0.98 : 0.99;
      emit(`bot${id}hi`, `ask ${lo.toFixed(2)}-${(lo + 0.01).toFixed(2)} (выше потолка)`);
    }
    for (const id of holding) {
      emit(`bot${id}`, "профиль");
      const rb = getRatio();
      const rc = getRatioOf("c");
      if (rb !== null) emit(`bot${id}b${binBucketIdx(rb)}`, "профиль");
      if (rc !== null) emit(`bot${id}c${binBucketIdx(rc)}`, "профиль");
      if (rb !== null && rc !== null) emit(`bot${id}k${(rb < 0 ? 1 : 0) + (rc < 0 ? 1 : 0)}`, "профиль");
    }

    return count;
  }

  // ───── v3.2 «Темка»: покупка в начале окна, продажа по тейк-профиту ─────

  private tstat(key: string): TStat {
    return this.temkaAgg[key] ?? (this.temkaAgg[key] = { n: 0, f: 0, w: 0, px: 0, roiH: 0, roiH2: 0, hit: 0, hitSec: 0, roiT: 0, roiT2: 0, nhW: 0 });
  }

  /**
   * В первые TEMKA_OPEN_MAX_SEC секунд окна берём живые стаканы обеих сторон и ставим «ордера» режимов «по рынку» и «лимитка»:
   * «по рынку» входит по живому ask сразу; лимитки входят сразу, только если ask уже не выше уровня, иначе ждут.
   * Если рынок замечен позже, он целиком пропускается, чтобы не смешивать входы на разном времени окна.
   * Режимы «контртренд» создаются позже, по сигналу (см. temkaCtCheck).
   */
  private async temkaTryOpen(market: CryptoUpDownMarket, p: PendingMarket): Promise<void> {
    if (p.temkaInit || this.temkaOpening.has(market.eventSlug)) return;
    const modes = TEMKA_MODES_BY_TF[market.windowMinutes];
    if (!modes) {
      p.temkaInit = true;
      p.temka = [];
      return;
    }
    const openMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
    const elapsed = (Date.now() - openMs) / 1000;
    if (elapsed < 0) return;
    if (elapsed > TEMKA_OPEN_MAX_SEC) {
      p.temkaInit = true;
      p.temka = [];
      return;
    }
    this.temkaOpening.add(market.eventSlug);
    try {
      const [up, down] = await Promise.all([fetchBook(market.upTokenId), fetchBook(market.downTokenId)]);
      if (!up || !down) return; // повторим на следующем тике / замере, пока не прошло TEMKA_OPEN_MAX_SEC
      if (this.pending.get(market.eventSlug) !== p || p.temkaInit) return;
      const t = Date.now();
      const list: TemkaPos[] = [];
      for (const [side, tok, b] of [
        ["Up", market.upTokenId, up],
        ["Down", market.downTokenId, down],
      ] as const) {
        this.quotes.set(tok, { bid: b.bid, ask: b.ask });
        for (const m of modes) {
          if (m.ctLevel !== undefined) continue;
          const pos: TemkaPos = {
            mode: m.id,
            side,
            level: m.level,
            deadlineMs: m.waitSec !== undefined ? openMs + m.waitSec * 1000 : undefined,
            filled: false,
            price: 0,
            fillTs: 0,
            hits: TEMKA_TP_DELTAS.map(() => null),
          };
          const ask = b.ask;
          if (ask !== null && b.askSize >= TEMKA_MIN_SIZE) {
            const okMarket = m.id === "m" && ask >= TEMKA_MARKET_ASK_MIN && ask <= TEMKA_MARKET_ASK_MAX;
            const okLimit = m.level !== undefined && ask > 0 && ask <= m.level + 1e-9;
            if (okMarket || okLimit) {
              pos.filled = true;
              pos.price = ask;
              pos.fillTs = t;
            }
          }
          list.push(pos);
        }
      }
      p.temka = list;
      p.temkaInit = true;
    } catch (err) {
      console.error("[temka] ошибка постановки:", (err as Error).message);
    } finally {
      this.temkaOpening.delete(market.eventSlug);
    }
  }

  /** На каждом тике токена: ищем «кандидатов» на исполнение лимитки или тейк-профита и подтверждаем живым стаканом. */
  private temkaTick(market: CryptoUpDownMarket, side: "Up" | "Down", p: PendingMarket, q: Quote, now: number): void {
    if (!p.temkaInit) {
      void this.temkaTryOpen(market, p);
      return;
    }
    const list = p.temka;
    if (!list || list.length === 0) return;
    this.temkaCtCheck(market, side, p, q, now);
    let candidate = false;
    for (const pos of list) {
      if (pos.side !== side) continue;
      if (!pos.filled) {
        if (pos.level === undefined || pos.deadlineMs === undefined || now > pos.deadlineMs) continue;
        if (q.ask !== null && q.ask <= pos.level + 1e-9) candidate = true;
      } else if (q.bid !== null) {
        for (let i = 0; i < TEMKA_TP_DELTAS.length; i++) {
          const target = pos.price + TEMKA_TP_DELTAS[i];
          if (pos.hits[i] === null && target <= 0.99 + 1e-9 && q.bid >= target - 1e-9) {
            candidate = true;
            break;
          }
        }
      }
      if (candidate) break;
    }
    if (candidate) void this.temkaConfirm(market, side, p);
  }

  /**
   * Живой стакан токена (REST): лимитка считается исполненной по своей цене, если ask опустился до уровня и на нём есть объём;
   * тейк-профит считается исполненным по цене цели, если живой bid не ниже цели и на нём есть объём.
   */
  private async temkaConfirm(market: CryptoUpDownMarket, side: "Up" | "Down", p: PendingMarket): Promise<void> {
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    if (this.temkaConfirming.has(tokenId)) return;
    if (Date.now() - (this.temkaLastConfirm.get(tokenId) ?? 0) < TEMKA_CONFIRM_COOLDOWN_MS) return;
    this.temkaConfirming.add(tokenId);
    this.temkaLastConfirm.set(tokenId, Date.now());
    try {
      const book = await fetchBook(tokenId);
      if (!book) return;
      this.quotes.set(tokenId, { bid: book.bid, ask: book.ask });
      if (book.ask !== null) this.pushAsk(tokenId, Date.now(), book.ask);
      if (this.pending.get(market.eventSlug) !== p || !p.temka) return;
      const t = Date.now();
      for (const pos of p.temka) {
        if (pos.side !== side) continue;
        if (!pos.filled) {
          if (pos.level === undefined || pos.deadlineMs === undefined || t > pos.deadlineMs) continue;
          if (book.ask !== null && book.ask <= pos.level + 1e-9 && book.askSize >= TEMKA_MIN_SIZE) {
            pos.filled = true;
            pos.price = pos.level;
            pos.fillTs = t;
          }
        } else if (book.bid !== null && book.bidSize >= TEMKA_MIN_SIZE) {
          for (let i = 0; i < TEMKA_TP_DELTAS.length; i++) {
            const target = pos.price + TEMKA_TP_DELTAS[i];
            if (pos.hits[i] === null && target <= 0.99 + 1e-9 && book.bid >= target - 1e-9) {
              pos.hits[i] = Math.max(0, (t - pos.fillTs) / 1000);
            }
          }
        }
      }
    } catch (err) {
      console.error("[temka] ошибка проверки:", (err as Error).message);
    } finally {
      this.temkaConfirming.delete(tokenId);
    }
  }

  /** Рывок монеты ПРОТИВ стороны за TEMKA_CT_LOOKBACK_SEC секунд в «сигмах» (плюс = монета идёт против токена). null — нет данных. */
  private temkaAgainstZ(market: CryptoUpDownMarket, side: "Up" | "Down", now: number): number | null {
    const feed = PRICE_FEEDS[market.coin];
    if (!feed) return null;
    const p0 = feed.getPriceAt(now - TEMKA_CT_LOOKBACK_SEC * 1000);
    const pn = feed.getLatestPrice();
    if (p0 === null || pn === null || p0 === 0) return null;
    const mv = (pn - p0) / p0;
    const against = side === "Up" ? -mv : mv;
    const vol = volTracker.getRecentVolatility(market.coin, now);
    if (vol === null || vol <= 1e-6) return null;
    return against / (vol * Math.sqrt(TEMKA_CT_LOOKBACK_SEC / 300));
  }

  /** «Контртренд»: токен подешевел до уровня после рывка монеты против него — кандидат на вход (подтверждает temkaCtConfirm). */
  private temkaCtCheck(market: CryptoUpDownMarket, side: "Up" | "Down", p: PendingMarket, q: Quote, now: number): void {
    const modes = TEMKA_MODES_BY_TF[market.windowMinutes];
    if (!modes || !p.temka || q.ask === null || q.ask <= 0) return;
    const maxLevel = Math.max(...TEMKA_CT_LEVELS);
    if (q.ask > maxLevel + 1e-9) return;
    const openMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
    if ((now - openMs) / 1000 < TEMKA_CT_MIN_ELAPSED_SEC || (market.closeTimeMs - now) / 1000 < TEMKA_CT_MIN_SEC_TO_CLOSE) return;
    const free = modes.filter(
      (m) => m.ctLevel !== undefined && (q.ask as number) <= m.ctLevel + 1e-9 && !(p.temka as TemkaPos[]).some((x) => x.mode === m.id && x.side === side),
    );
    if (free.length === 0) return;
    const z = this.temkaAgainstZ(market, side, now);
    if (z === null || z < Math.min(...TEMKA_CT_ZS)) return;
    void this.temkaCtConfirm(market, side, p);
  }

  private async temkaCtConfirm(market: CryptoUpDownMarket, side: "Up" | "Down", p: PendingMarket): Promise<void> {
    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    const key = `ct:${tokenId}`;
    if (this.temkaConfirming.has(key)) return;
    if (Date.now() - (this.temkaLastConfirm.get(key) ?? 0) < TEMKA_CONFIRM_COOLDOWN_MS) return;
    this.temkaConfirming.add(key);
    this.temkaLastConfirm.set(key, Date.now());
    try {
      const modes = TEMKA_MODES_BY_TF[market.windowMinutes];
      const book = await fetchBook(tokenId);
      if (!book || !modes) return;
      this.quotes.set(tokenId, { bid: book.bid, ask: book.ask });
      if (book.ask !== null) this.pushAsk(tokenId, Date.now(), book.ask);
      if (this.pending.get(market.eventSlug) !== p || !p.temka) return;
      const t = Date.now();
      const openMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
      if ((t - openMs) / 1000 < TEMKA_CT_MIN_ELAPSED_SEC || (market.closeTimeMs - t) / 1000 < TEMKA_CT_MIN_SEC_TO_CLOSE) return;
      if (book.ask === null || book.ask <= 0 || book.askSize < TEMKA_MIN_SIZE) return;
      const z = this.temkaAgainstZ(market, side, t);
      if (z === null) return;
      for (const m of modes) {
        if (m.ctLevel === undefined || m.ctZ === undefined) continue;
        if (book.ask > m.ctLevel + 1e-9 || z < m.ctZ) continue;
        if (p.temka.some((x) => x.mode === m.id && x.side === side)) continue;
        p.temka.push({
          mode: m.id,
          side,
          filled: true,
          price: book.ask,
          fillTs: t,
          hits: TEMKA_TP_DELTAS.map(() => null),
        });
      }
    } catch (err) {
      console.error("[temka] ошибка контртренда:", (err as Error).message);
    } finally {
      this.temkaConfirming.delete(key);
    }
  }

  /** Резолв рынка: считаем итог каждой «ордер-позиции» и пишем в общую статистику, по стороне, по монете и по монете+стороне. */
  private temkaSettle(p: PendingMarket, winner: "Up" | "Down"): void {
    if (!p.temka) return;
    const tf = p.wm ?? 5;
    for (const pos of p.temka) {
      const filled = pos.filled && pos.price > 0;
      const won = pos.side === winner;
      const roiH = filled ? (won ? (1 - pos.price) / pos.price : -1) : 0;
      const mk = temkaKeyMode(pos.mode, tf);
      for (const ck of [p.coin, "*"]) {
        for (const sk of [pos.side, "*"]) {
          const suffix = `${ck}|${sk}`;
          const h = this.tstat(`${mk}|h|${suffix}`);
          h.n++;
          if (!filled) continue;
          h.f++;
          if (won) h.w++;
          h.px += pos.price;
          h.roiH += roiH;
          h.roiH2 += roiH * roiH;
          TEMKA_TP_DELTAS.forEach((d, i) => {
            if (pos.price + d > 0.99 + 1e-9) return;
            const s = this.tstat(`${mk}|${i}|${suffix}`);
            const hs = pos.hits[i];
            const hit = hs !== null && hs !== undefined;
            const r = hit ? d / pos.price : roiH;
            s.f++;
            s.roiH += roiH;
            s.roiT += r;
            s.roiT2 += r * r;
            if (hit) {
              s.hit++;
              s.hitSec += hs as number;
            } else if (won) {
              s.nhW++;
            }
          });
        }
      }
    }
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
          void this.temkaTryOpen(m, p);
          if (m.windowMinutes !== TARGET_WINDOW_MINUTES) return; // арбитраж и честность цен считаем только по 5м
          const [up, down] = await Promise.all([fetchBook(m.upTokenId), fetchBook(m.downTokenId)]);
          if (!up || !down) return;

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
            if (b.ask !== null) this.pushAsk(tok, Date.now(), b.ask);
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

  // ───── v3.6: «как бы покупает» ─────

  /** Куда отправлять текст о бумажной покупке/итоге (ставится в main: Telegram). */
  onPaperText: ((text: string) => void) | null = null;

  private paperBuy(idea: string, market: CryptoUpDownMarket, side: "Up" | "Down", e: LabEntry, ratio: number | null): void {
    const cfg = PAPER_TRADE[idea];
    const shares = cfg.size / cfg.sizeRefPrice;
    const t = new Date(e.ts).toISOString().slice(11, 23);
    const text =
      `📝 LAB БЫ КУПИЛ [${idea}, $${cfg.size}] ${t} UTC\n` +
      `${COIN_SHORT[market.coin] ?? market.coin} ${market.windowMinutes}м ${side}, ask ${e.price.toFixed(3)}, bid ${e.bid0 != null ? e.bid0.toFixed(3) : "—"} (потолок бота ${cfg.cap})\n` +
      `до закрытия ${e.sec}с, z ${e.z != null ? e.z.toFixed(2) : "—"}, Binance ${ratio !== null ? `${(ratio * 100).toFixed(0)}%` : "—"} от потока, ~${shares.toFixed(2)} акций\n` +
      `${market.eventSlug}`;
    console.log(text.replace(/\n/g, " | "));
    this.onPaperText?.(text);
  }

  // ───── v3.7: реальная торговля ─────

  private clob: ClobService | null = null;
  liveEnabled = false; // рантайм-переключатель («лаб торговля вкл/выкл»)
  private liveSlugs = new Set<string>(); // рынки, где уже была попытка живой покупки
  private liveFills = new Map<string, { idea: string; coin: string; side: "Up" | "Down"; shares: number; avg: number; usd: number }>();
  private liveWindowCount = new Map<number, number>();
  private liveTimes: number[] = [];
  private liveStats = { bought: 0, failed: 0, skipped: 0, wins: 0, losses: 0, pnl: 0 };

  setClob(c: ClobService): void {
    this.clob = c;
  }
  hasClob(): boolean {
    return this.clob !== null;
  }

  private note(text: string): void {
    console.log(text.replace(/\n/g, " | "));
    this.onPaperText?.(text);
  }

  liveStatusText(): string {
    const s = this.liveStats;
    return (
      `Торговля LAB: ${this.liveEnabled && this.clob ? "🟢 РЕАЛЬНАЯ" : this.clob ? "⏸ выключена (бумажная)" : "⚪ не подключена (бумажная)"}\n` +
      `Идеи: ${Object.entries(PAPER_TRADE).map(([k, v]) => `${k} $${v.size}`).join(", ")}\n` +
      `Куплено ${s.bought}, не удалось ${s.failed}, пропущено по лимитам ${s.skipped}; закрыто: ${s.wins} побед / ${s.losses} проигрышей, итог ${s.pnl >= 0 ? "+" : "−"}$${Math.abs(s.pnl).toFixed(2)}\n` +
      `Открыто сейчас: ${this.liveFills.size} (лимит ${LIVE_MAX_OPEN}); за последний час покупок: ${this.liveTimes.filter((t) => Date.now() - t < 3600_000).length}/${LIVE_QUOTA_PER_HOUR}`
    );
  }

  private async liveBuy(idea: string, market: CryptoUpDownMarket, side: "Up" | "Down", e: LabEntry, ratio: number | null): Promise<void> {
    const cfg = PAPER_TRADE[idea];
    const slug = market.eventSlug;
    const name = `${COIN_SHORT[market.coin] ?? market.coin} ${market.windowMinutes}м ${side}`;
    const head = `[${idea}, $${cfg.size}] ${name}, ask ${e.price.toFixed(3)}, bid ${e.bid0 != null ? e.bid0.toFixed(3) : "—"}, z ${e.z != null ? e.z.toFixed(2) : "—"}, Binance ${ratio !== null ? `${(ratio * 100).toFixed(0)}%` : "—"}, до закрытия ${e.sec}с`;
    if (!this.clob) return;
    if (this.liveSlugs.has(slug)) return; // одна позиция на рынок
    const now = Date.now();
    this.liveTimes = this.liveTimes.filter((t) => now - t < 3600_000);
    const limit =
      this.liveFills.size + this.pendingLive >= LIVE_MAX_OPEN
        ? `открыто ${this.liveFills.size} (лимит ${LIVE_MAX_OPEN})`
        : (this.liveWindowCount.get(market.closeTimeMs) ?? 0) >= LIVE_MAX_PER_WINDOW
          ? `в этом окне уже ${LIVE_MAX_PER_WINDOW} покупки`
          : this.liveTimes.length >= LIVE_QUOTA_PER_HOUR
            ? `квота ${LIVE_QUOTA_PER_HOUR} в час`
            : null;
    if (limit !== null) {
      this.liveSlugs.add(slug);
      this.liveStats.skipped++;
      this.note(`⛔ LAB НЕ КУПИЛ (лимит: ${limit})\n${head}\n${slug}`);
      return;
    }
    this.liveSlugs.add(slug);
    this.pendingLive++;
    this.liveWindowCount.set(market.closeTimeMs, (this.liveWindowCount.get(market.closeTimeMs) ?? 0) + 1);

    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    const size = cfg.size / cfg.sizeRefPrice;
    const errors: string[] = [];
    const t0 = Date.now();
    try {
      for (let attempt = 1; attempt <= LIVE_ATTEMPTS; attempt++) {
        const left = (market.closeTimeMs - Date.now()) / 1000;
        if (left < LIVE_MIN_SEC_LEFT) {
          errors.push(`попытка ${attempt}: до закрытия осталось ${left.toFixed(1)}с — не пытаюсь`);
          break;
        }
        const snapAge = Date.now() - e.ts;
        const tAttempt = Date.now();
        try {
          const resp = await this.clob.placeLimitOrder({
            tokenId,
            side: Side.BUY,
            price: e.price,
            size,
            maxSlippagePct: 0.5,
            maxBuyPrice: cfg.cap,
            minBidPrice: cfg.minBid,
            snapshot: attempt === 1 && e.bid0 != null && snapAge <= 1500 ? { bid: e.bid0, ask: e.price, ageMs: snapAge } : undefined,
            priceAtCap: true,
          });
          const rawA = Number(resp.filledSize ?? 0);
          const rawB = Number(resp.filledUsdc ?? 0);
          const { filledSize, avgPrice } = resolveFillLive(rawA, rawB, e.price);
          if (filledSize <= 0) {
            errors.push(`попытка ${attempt}: биржа ответила без заполнения (сырые ${rawA}/${rawB})`);
          } else {
            const usd = filledSize * avgPrice;
            this.liveFills.set(slug, { idea, coin: e.coin, side, shares: filledSize, avg: avgPrice, usd });
            this.liveTimes.push(Date.now());
            this.liveStats.bought++;
            this.note(
              `💰 LAB КУПИЛ ${head}\n` +
                `${filledSize.toFixed(2)} акций по ${avgPrice.toFixed(4)} (≈$${usd.toFixed(2)}), попытка ${attempt}/${LIVE_ATTEMPTS}, ордер ${Date.now() - tAttempt} мс, от сигнала ${Date.now() - e.ts} мс${resp.timing ? `, путь ${resp.timing.path === "fast" ? "быстрый" : "обычный"}` : ""}\n${slug}`,
            );
            return;
          }
        } catch (err) {
          errors.push(`попытка ${attempt}: ${(err as Error).message}`);
        }
        if (attempt < LIVE_ATTEMPTS) await new Promise((r) => setTimeout(r, LIVE_RETRY_DELAY_MS));
      }
      this.liveStats.failed++;
      this.liveWindowCount.set(market.closeTimeMs, Math.max(0, (this.liveWindowCount.get(market.closeTimeMs) ?? 1) - 1));
      this.note(`🚫 LAB НЕ СМОГ КУПИТЬ ${head}\nПотрачено ${Date.now() - t0} мс, причины:\n${errors.map((x) => `• ${x}`).join("\n")}\n${slug}`);
    } catch (err) {
      this.liveStats.failed++;
      this.note(`🚫 LAB: ошибка при покупке ${head}\n${(err as Error).message}\n${slug}`);
    } finally {
      this.pendingLive--;
    }
  }
  private pendingLive = 0;

  private liveSettle(slug: string, winner: "Up" | "Down"): boolean {
    const f = this.liveFills.get(slug);
    if (!f) return false;
    this.liveFills.delete(slug);
    const won = f.side === winner;
    const pnl = won ? f.shares * (1 - f.avg) : -f.usd;
    this.liveStats.pnl += pnl;
    if (won) this.liveStats.wins++;
    else this.liveStats.losses++;
    this.note(`${won ? "✅" : "❌"} LAB итог [${f.idea}] ${COIN_SHORT[f.coin] ?? f.coin} ${f.side} по ${f.avg.toFixed(3)}: ${won ? "победа" : "проигрыш"} ${pnl >= 0 ? "+" : "−"}$${Math.abs(pnl).toFixed(2)} (всего ${this.liveStats.pnl >= 0 ? "+" : "−"}$${Math.abs(this.liveStats.pnl).toFixed(2)}) ${slug}`);
    return true;
  }

  private paperSettle(p: PendingMarket, slug: string, winner: "Up" | "Down"): void {
    const wasBought = this.liveSettle(slug, winner);
    for (const e of p.entries) {
      const cfg = PAPER_TRADE[e.idea];
      if (cfg === undefined) continue;
      const won = e.side === winner;
      if (this.liveSlugs.has(slug)) {
        // живая попытка была: если не купили — сообщаем, сколько упустили/сэкономили
        if (!wasBought) {
          const sh = cfg.size / cfg.sizeRefPrice;
          const pn = won ? sh * (1 - e.price) : -sh * e.price;
          this.note(`ℹ️ LAB: рынок ${slug} закрыт, мы там НЕ купили; ${won ? "упущено" : "избежали"} ${pn >= 0 ? "+" : "−"}$${Math.abs(pn).toFixed(2)}`);
        }
        continue;
      }
      const shares = cfg.size / cfg.sizeRefPrice;
      const pnl = won ? shares * (1 - e.price) : -shares * e.price;
      const text = `${won ? "✅" : "❌"} LAB БЫ: итог [${e.idea}] ${COIN_SHORT[e.coin] ?? e.coin} ${e.side} по ${e.price.toFixed(3)}: ${won ? "победа" : "проигрыш"} ${pnl >= 0 ? "+" : "−"}$${Math.abs(pnl).toFixed(2)} (${slug})`;
      console.log(text);
      this.onPaperText?.(text);
    }
  }

  // ───── v3.3: лента цен и карточки рынков ─────

  /** Раз в 5с пишет в каждый живой рынок строку ленты: bid/ask обеих сторон, монета и биржи (bps от открытия окна). */
  private sampleTape(): void {
    const now = Date.now();
    for (const m of this.activeMarkets.values()) {
      if (m.closeTimeMs + 3000 < now) continue;
      const openMs = m.closeTimeMs - m.windowMinutes * 60 * 1000;
      if (now < openMs) continue;
      const p = this.pending.get(m.eventSlug);
      if (!p) continue;
      const feed = PRICE_FEEDS[m.coin];
      if (!feed) continue;
      let op = this.openPrices.get(m.eventSlug);
      if (op === undefined) {
        const o = feed.getPriceAt(openMs);
        if (o === null) continue;
        op = o;
        this.openPrices.set(m.eventSlug, o);
      }
      const nowP = feed.getLatestPrice();
      const bps = (cur: number | null, o: number | null): number | null =>
        cur === null || o === null || o === 0 ? null : Math.round(((cur - o) / o) * 1e6) / 100;
      const qu = this.quotes.get(m.upTokenId);
      const qd = this.quotes.get(m.downTokenId);
      const r3 = (x: number | null | undefined): number | null => (x === null || x === undefined ? null : Math.round(x * 1000) / 1000);
      const row: TapeRow = [
        Math.round(((now - openMs) / 1000) * 10) / 10,
        r3(qu?.bid),
        r3(qu?.ask),
        r3(qd?.bid),
        r3(qd?.ask),
        bps(nowP, op),
        bps(binance.getLatest(m.coin), binance.getPriceAt(m.coin, openMs)),
        bps(coinbase.getLatest(m.coin), coinbase.getPriceAt(m.coin, openMs)),
      ];
      (p.tape ??= []).push(row);
    }
  }

  /** Записывает карточку рынка на диск: лента + все сделки идей (без «корзин»: Binance/Coinbase-корзина — фильтр на странице). */
  private recordMarket(slug: string, p: PendingMarket, winner: "Up" | "Down"): void {
    try {
      const wm = p.wm ?? 5;
      const open = p.closeTimeMs - wm * 60 * 1000;
      const trades: RawTrade[] = [];
      const variant = /^(modelb\d|modelbW|modelbS|modelc\d|modelk\d|fair2b0|bot\d+[bck]\d)$/;
      for (const e of p.entries) {
        if (variant.test(e.idea)) continue;
        trades.push({
          i: e.idea,
          c: e.combo,
          s: e.side === "Up" ? 0 : 1,
          ts: e.ts,
          px: e.price,
          sec: e.sec,
          bid0: e.bid0 ?? null,
          z: e.z ?? null,
          pm: e.pm ?? null,
          sp10: e.sp10 ?? null,
          sp30: e.sp30 ?? null,
          bin: e.bin ?? null,
          cb: e.cb ?? null,
          xs: e.xs,
          rv2: e.rv2 ?? null,
          rv5: e.rv5 ?? null,
          cg: e.cg ?? null,
          cgs: e.cgs ?? null,
          tr15: e.tr15 ?? null,
          fe: e.fe ?? null,
          xings: e.xings ?? null,
          agreeN: e.agreeN ?? null,
          c05: e.c05,
          c10: e.c10,
          hr: e.hr ?? null,
          pk: e.pk ?? null,
          bsz: e.bsz ?? null,
          asz: e.asz ?? null,
        });
      }
      for (const pos of p.temka ?? []) {
        if (!pos.filled || pos.price <= 0) continue;
        trades.push({
          i: `temka:${pos.mode}`,
          c: `${wm}м`,
          s: pos.side === "Up" ? 0 : 1,
          ts: pos.fillTs,
          px: pos.price,
          sec: Math.round((p.closeTimeMs - pos.fillTs) / 1000),
          hits: pos.hits,
        });
      }
      if (trades.length === 0) return;
      const rec: MarketRec = {
        v: 1,
        slug,
        coin: p.coin,
        wm,
        open,
        close: p.closeTimeMs,
        win: winner === "Up" ? 0 : 1,
        tape: p.tape ?? [],
        trades,
      };
      appendMarketRecord(LAB_DATA_DIR, rec);
    } catch (err) {
      console.error("[lab-data] ошибка записи карточки:", (err as Error).message);
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

    // признаки, Binance, согласие профилей, просадка и стопы — по базовым входам профилей бота
    for (const e of p.entries) {
      if (!isBase(e.idea)) continue;
      const wonE = e.side === winner;
      this.aggregateBotEntry(e, wonE, wonE ? (1 - e.price) / e.price : -1);
    }

    // v3.1: кластеры проигрышей — сколько монет проиграло в одном и том же окне
    const baseEntries = p.entries.filter((e) => isBase(e.idea));
    if (baseEntries.length > 0) {
      const wk = String(p.closeTimeMs);
      const lw = this.lossWin[wk] ?? (this.lossWin[wk] = { lost: [], seen: [], rvSum: 0, rvN: 0 });
      if (!lw.seen.includes(p.coin)) lw.seen.push(p.coin);
      if (baseEntries.some((e) => e.side !== winner) && !lw.lost.includes(p.coin)) lw.lost.push(p.coin);
      for (const e of baseEntries) {
        if (e.rv2 != null && Number.isFinite(e.rv2)) {
          lw.rvSum += e.rv2;
          lw.rvN++;
        }
      }
      const keys = Object.keys(this.lossWin);
      if (keys.length > LOSS_WINDOWS_MAX) {
        keys.sort((a, b) => Number(a) - Number(b));
        for (const k of keys.slice(0, keys.length - LOSS_WINDOWS_MAX)) delete this.lossWin[k];
      }
    }

    const earliest = new Map<string, LabEntry>(); // `${idea}|${side}` -> самый ранний вход
    for (const e of p.entries) {
      const won = e.side === winner;
      const roi = won ? (1 - e.price) / e.price : -1;
      bump(this.agg, `${e.idea}|${e.combo}|${e.coin}`, won, roi, e.price);

      // калибровка «справедливой цены»: предсказанная p против фактического исхода
      if (e.idea.startsWith("fair") && e.fp != null && e.fe != null) {
        bump(this.fairCal, `${e.idea}|${FAIR_EDGE_LABELS[fairIdx(e.fe)]}`, won, roi, e.fp);
      }

      // журнал сделок реального бота (идеи из LIVE_VARIANTS) — для отчёта по периодам и в долларах
      const size = LIVE_VARIANTS[e.idea];
      if (size !== undefined) {
        const shares = size / BOT_ENTRY_PRICE;
        const pnl = won ? shares * (1 - e.price) : -shares * e.price;
        this.liveLog.push({
          ts: e.ts,
          idea: e.idea,
          coin: e.coin,
          side: e.side,
          price: e.price,
          won,
          pnl,
          z: this.liveZ(p, e),
          ratio: this.liveRatio(p, e),
        });
        if (this.liveLog.length > LIVE_LOG_MAX) this.liveLog.shift();
      }

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
      if (!isBase(e.idea) || e.xings === undefined) continue;
      const won = e.side === winner;
      const roi = won ? (1 - e.price) / e.price : -1;
      const n = Math.min(e.xings, 3);
      bump(this.saw, `${e.idea}|x${n}`, won, roi, e.price);
      bump(this.saw, `all|${e.coin}|x${n}`, won, roi, e.price);
    }

    // согласие профилей бота — по каждой стороне рынка
    const botBySide = new Map<string, LabEntry[]>();
    for (const e of earliest.values()) {
      if (!isBase(e.idea)) continue;
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

    this.temkaSettle(p, winner);
    this.recordMarket(slug, p, winner);
    this.paperSettle(p, slug, winner);

    this.dropPending(slug);
    if ((p.wm ?? 5) === TARGET_WINDOW_MINUTES) this.resolvedCount++;
  }

  /** z и «во сколько раз Binance прошёл от потока» для записи реального бота: берём из базового входа того же профиля на том же рынке. */
  private liveBase(p: PendingMarket, e: LabEntry): LabEntry | undefined {
    const m = e.idea.match(/^bot(\d+)/);
    if (!m) return undefined;
    return p.entries.find((x) => x.idea === `bot${m[1]}` && x.side === e.side);
  }
  private liveZ(p: PendingMarket, e: LabEntry): number | null {
    const b = this.liveBase(p, e);
    return b && b.z !== undefined ? b.z : null;
  }
  private liveRatio(p: PendingMarket, e: LabEntry): number | null {
    const b = this.liveBase(p, e);
    return b && b.bin !== null && b.bin !== undefined && b.pm !== undefined && b.pm > 0 ? b.bin / b.pm : null;
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
    const prefix = `${idea}|`;
    for (const [key, s] of Object.entries(this.agg)) {
      if (!key.startsWith(prefix)) continue;
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
    lines.push(`Каждая комбинация записывает каждый сигнал (${RULES_TEXT}). Цена входа проверяется живым стаканом (REST) перед записью.`);
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
    lines.push(`Цена входа = живой ask (проверяется REST перед записью). Правила: ${RULES_TEXT}. Держим до резолва.`);
    lines.push("");
    lines.push("Как читать:");
    lines.push(" • Сила — во сколько раз монета прошла больше обычного для оставшегося времени (1 = обычное, 3+ = очень сильно)");
    lines.push(" • Цена — средняя цена токена на входе");
    lines.push(" • Запас — реальный винрейт минус цена. Плюс = выгодно");
    lines.push(" • Надёжно — осторожная оценка винрейта с поправкой на малую выборку");
    lines.push(" • ✅ надёжно выше цены (выгодно даже по осторожной оценке); ⚠️ винрейт выше цены, но данных мало, может быть удача; ❌ невыгодно");
    lines.push(" • Один рынок может попасть в несколько комбинаций по мере того, как идёт время и меняется цена");
    lines.push(' • Та же сетка с фильтрами бирж: "лаб модель бинанс"');
    lines.push("");

    lines.push(`<b>── Все ячейки (${allRows.length}), даже с 1 сделкой; по порядку: сила / время / цена ──</b>`);
    lines.push("Разбор каждой ячейки (чем победы отличаются от поражений, путь цены после входа): «лаб страница».");
    if (allRows.length === 0) lines.push("  пока нет данных");
    else [...allRows].sort((x, y) => x.combo.localeCompare(y.combo, "ru", { numeric: true })).forEach((r) => lines.push(this.modelLine(null, r)));
    lines.push("");

    lines.push(`<b>── По монетам: лучшая комбинация (минимум ${MODEL_MIN_COIN} записей) ──</b>`);
    const stats = this.collect("model");
    for (const coin of INCLUDED_COINS) {
      const coinRows = this.toModelRows(stats.filter((s) => s.coin === coin)).filter((r) => r.t >= MODEL_MIN_COIN);
      const top = coinRows.sort((a, b) => b.roiAvg - a.roiAvg || b.t - a.t)[0];
      lines.push(top ? `  ${COIN_SHORT[coin]}:${this.modelLine(null, top).slice(1)}` : `  ${COIN_SHORT[coin]}: пока мало данных`);
    }
    lines.push("");

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

  /** v3.1 / A: Binance-против (и Coinbase, и обе биржи) на всей сетке модели + «доза-эффект». */
  buildModelBinReport(): string {
    // индексируем один раз: идея -> сводные строки по комбинациям
    const cache = new Map<string, PooledRow[]>();
    const rowsOf = (idea: string): PooledRow[] => {
      let r = cache.get(idea);
      if (!r) {
        r = this.pooled(idea);
        cache.set(idea, r);
      }
      return r;
    };
    const sumLabels = (idea: string, idx: number, labels: string[] | null): { w: number; t: number; roi: number; px: number } => {
      let w = 0;
      let t = 0;
      let roi = 0;
      let px = 0;
      for (const r of rowsOf(idea)) {
        if (labels !== null && !labels.includes(r.combo.split(" / ")[idx] ?? "")) continue;
        w += r.w;
        t += r.t;
        roi += r.roi;
        px += r.px;
      }
      return { w, t, roi, px };
    };
    const line = (label: string, s: { w: number; t: number; roi: number; px: number }): string => {
      if (s.t === 0) return `  ${label}: пока нет данных`;
      const avg = s.px / s.t;
      const lo = wilsonLower(s.w, s.t);
      const verdict = lo > avg ? "✅" : s.w / s.t > avg ? "⚠️" : "❌";
      return `  ${label}: ${s.w}/${s.t} (${winPct(s.w, s.t)}), надёжно ${(lo * 100).toFixed(0)}%, цена ${avg.toFixed(2)}, запас ${signedPct(s.w / s.t - avg)}, прибыль ${roiLabel(s.roi, s.t)} ${verdict}${s.t < 30 ? " (мало данных)" : ""}`;
    };

    const lines: string[] = [];
    lines.push("<b>📊 Модель цены + фильтр бирж (вся сетка)</b>");
    lines.push(
      "Те же ячейки модели (сила × секунды × цена), но запись делается отдельно по состоянию бирж в момент входа. Binance-против = Binance уже за ценой открытия окна, хотя поток бота в нашу сторону. «Чуть против» = Binance в диапазоне 0…−0.25 от движения потока, «сильно против» = ниже −0.25. Фильтр — условие входа с ожиданием, как в боте. Счёт с момента обновления до v3.1.",
    );
    lines.push(`Опрос Binance: удачно ${binance.okCount}, ошибок ${binance.failCount}. Coinbase: удачно ${coinbase.okCount}, ошибок ${coinbase.failCount}.`);
    lines.push("");

    lines.push("<b>── Итого по всей сетке ──</b>");
    lines.push(line("без фильтра", sumLabels("model", 0, null)));
    lines.push(line("Binance против (все)", sumLabels("modelb0", 0, null)));
    lines.push(line("   из них чуть против (0…−0.25)", sumLabels("modelbW", 0, null)));
    lines.push(line("   из них сильно против (< −0.25)", sumLabels("modelbS", 0, null)));
    for (let i = 1; i < BIN_LABELS.length; i++) lines.push(line(BIN_LABELS[i], sumLabels(`modelb${i}`, 0, null)));
    lines.push("");

    // Лесенка и доза-эффект
    lines.push("<b>── Проверка гипотез ──</b>");
    const ladder = [0, 1, 2, 3, 4].map((i) => ({ i, s: sumLabels(`modelb${i}`, 0, null) }));
    const ok = ladder.filter((x) => x.s.t >= 20);
    if (ok.length < 3) {
      lines.push("  лесенка по корзинам Binance: мало данных (нужно ≥3 корзины с 20+ записями)");
    } else {
      let mono = true;
      for (let k = 1; k < ok.length; k++) if (ok[k].s.w / ok[k].s.t > ok[k - 1].s.w / ok[k - 1].s.t + 0.005) mono = false;
      lines.push(
        `  лесенка винрейта «против → >120%»: ${ok.map((x) => `${RATIO_SHORT[x.i]} ${winPct(x.s.w, x.s.t)}`).join(" → ")} ${mono ? "✅ гладкая (падает вниз)" : "— не монотонная"}`,
      );
    }
    const wk = sumLabels("modelbW", 0, null);
    const st = sumLabels("modelbS", 0, null);
    if (wk.t < 30 || st.t < 30) {
      lines.push(`  доза-эффект: мало данных (чуть против ${wk.t}, сильно против ${st.t}; нужно ≥30 в каждой)`);
    } else {
      const wrW = wk.w / wk.t;
      const wrS = st.w / st.t;
      const better = wrS > wrW && st.roi / st.t > wk.roi / wk.t;
      lines.push(
        `  доза-эффект: чуть против ${winPct(wk.w, wk.t)} (${wk.t}), сильно против ${winPct(st.w, st.t)} (${st.t}) → ${better ? "✅ «сильно» лучше — идея живая" : "❌ «сильно» не лучше — доказательства нет"}`,
      );
    }
    const base = sumLabels("model", 0, null);
    const b0 = sumLabels("modelb0", 0, null);
    if (base.t > 0 && b0.t > 0) {
      lines.push(
        `  по всей сетке: без фильтра запас ${signedPct(base.w / base.t - base.px / base.t)}, с Binance против ${signedPct(b0.w / b0.t - b0.px / b0.t)}`,
      );
    }
    lines.push("");

    // Срез по цене токена: именно здесь проверяется гипотеза «на дешёвых ценах 0.85-0.93 эффект жирнее»
    lines.push("<b>── Срез по цене токена: без фильтра | Binance против | сильно против | Binance >120% ──</b>");
    for (const band of MODEL_PRICE_BANDS) {
      const lb = priceLabel(band);
      const a = sumLabels("model", 2, [lb]);
      const b = sumLabels("modelb0", 2, [lb]);
      const c = sumLabels("modelbS", 2, [lb]);
      const d = sumLabels("modelb4", 2, [lb]);
      if (a.t === 0 && b.t === 0) continue;
      const cell = (s: { w: number; t: number; roi: number; px: number }): string =>
        s.t === 0 ? "—" : `${s.w}/${s.t} ${winPct(s.w, s.t)}, запас ${signedPct(s.w / s.t - s.px / s.t)}, приб. ${roiLabel(s.roi, s.t)}`;
      lines.push(`  ${lb}: ${cell(a)} | ${cell(b)} | ${cell(c)} | ${cell(d)}`);
    }
    lines.push("");
    const cheapLabels = ["0.85-0.90", "0.90-0.93"];
    lines.push("<b>── Дешёвые цены 0.85-0.93 (там эффект должен быть жирнее) ──</b>");
    lines.push(line("без фильтра", sumLabels("model", 2, cheapLabels)));
    lines.push(line("Binance против", sumLabels("modelb0", 2, cheapLabels)));
    lines.push(line("   чуть против", sumLabels("modelbW", 2, cheapLabels)));
    lines.push(line("   сильно против", sumLabels("modelbS", 2, cheapLabels)));
    lines.push(line("Coinbase против", sumLabels("modelc0", 2, cheapLabels)));
    lines.push(line("обе биржи против", sumLabels("modelk2", 2, cheapLabels)));
    lines.push("");
    lines.push("<b>── Коридор бота 0.93-0.96 (для сравнения) ──</b>");
    lines.push(line("без фильтра", sumLabels("model", 2, ["0.93-0.96"])));
    lines.push(line("Binance против", sumLabels("modelb0", 2, ["0.93-0.96"])));
    lines.push(line("   сильно против", sumLabels("modelbS", 2, ["0.93-0.96"])));
    lines.push("");

    lines.push("<b>── Coinbase и две биржи сразу (вся сетка) ──</b>");
    for (let i = 0; i < CB_LABELS.length; i++) lines.push(line(CB_LABELS[i], sumLabels(`modelc${i}`, 0, null)));
    for (let i = K_LABELS.length - 1; i >= 0; i--) lines.push(line(K_LABELS[i], sumLabels(`modelk${i}`, 0, null)));
    lines.push("");

    lines.push(`<b>── Лучшие 10 ячеек при Binance против (минимум ${MODEL_MIN_COIN} записей) ──</b>`);
    const bestB = this.toModelRows(rowsOf("modelb0"))
      .filter((r) => r.t >= MODEL_MIN_COIN)
      .sort((a, b) => b.roiAvg - a.roiAvg || b.t - a.t)
      .slice(0, 10);
    if (bestB.length === 0) lines.push("  пока недостаточно данных");
    else bestB.forEach((r, i) => lines.push(this.modelLine(i + 1, r)));
    lines.push("");

    lines.push("<b>── По монетам: Binance против, вся сетка ──</b>");
    const byCoinB = this.collect("modelb0");
    const byCoinBase = this.collect("model");
    for (const coin of INCLUDED_COINS) {
      const agg = (list: { coin: string; w: number; t: number; roi: number; px: number }[]) =>
        list.filter((r) => r.coin === coin).reduce((a, r) => ({ w: a.w + r.w, t: a.t + r.t, roi: a.roi + r.roi, px: a.px + r.px }), { w: 0, t: 0, roi: 0, px: 0 });
      lines.push(line(`${COIN_SHORT[coin]} против`, agg(byCoinB)));
      lines.push(line(`${COIN_SHORT[coin]} без фильтра`, agg(byCoinBase)));
    }
    lines.push("");
    lines.push("Прибыль — средний результат на $1 ставки (без комиссий). Одна и та же ситуация может попасть в несколько ячеек подряд, поэтому доверяй устойчивым срезам, а не отдельным ячейкам.");
    return lines.join("\n");
  }

  // ───── v3.1 / D: «Справедливая цена» ─────

  buildFairReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Справедливая цена: p = Φ(d / (σ·√t))</b>");
    lines.push(
      "d — расстояние монеты от цены открытия в нашу сторону (в долях цены), t — секунд до закрытия, σ — реальная секундная волатильность из тиков фида за последние 60 или 120 секунд (не ниже 30% от обычной, чтобы «тихий» фид не давал фальшивую уверенность). edge = p − ask. Пишутся все входы в диапазоне ask 0.60-0.99, до 240с до закрытия, при движении в нашу сторону; корзины edge: <0 (контроль), 0-2%, 2-4%, 4-6%, 6%+.",
    );
    lines.push("Провал идеи: винрейт не растёт вместе с корзинами edge. Цена в строке — средний ask входа, «модель p» — что обещала модель, «запас» — факт минус цена.");
    lines.push("");

    const bucketStat = (idea: string, rows: PooledRow[], i: number) => {
      let w = 0;
      let t = 0;
      let roi = 0;
      let px = 0;
      for (const r of rows) {
        if (r.combo.split(" / ")[0] !== FAIR_EDGE_LABELS[i]) continue;
        w += r.w;
        t += r.t;
        roi += r.roi;
        px += r.px;
      }
      const cal = this.fairCal[`${idea}|${FAIR_EDGE_LABELS[i]}`];
      const avgP = cal && cal.t > 0 ? (cal.px ?? 0) / cal.t : null;
      return { w, t, roi, px, avgP };
    };
    const fmt = (label: string, s: { w: number; t: number; roi: number; px: number; avgP?: number | null }): string => {
      if (s.t === 0) return `  ${label}: пока нет данных`;
      const avg = s.px / s.t;
      const lo = wilsonLower(s.w, s.t);
      const verdict = lo > avg ? "✅" : s.w / s.t > avg ? "⚠️" : "❌";
      return `  ${label}: ${s.w}/${s.t} (${winPct(s.w, s.t)}), цена ${avg.toFixed(3)}${s.avgP != null ? `, модель p ${(s.avgP * 100).toFixed(1)}%` : ""}, запас ${signedPct(s.w / s.t - avg)}, надёжно ${(lo * 100).toFixed(0)}%, прибыль ${roiLabel(s.roi, s.t)} ${verdict}${s.t < 30 ? " (мало данных)" : ""}`;
    };

    for (const fv of FAIR_VARIANTS) {
      const rows = this.pooled(fv.idea);
      const total = rows.reduce((a, r) => a + r.t, 0);
      lines.push(`<b>══ ${fv.idea}: ${fv.label} (записей ${total}) ══</b>`);
      const stats = FAIR_SHORT.map((_, i) => bucketStat(fv.idea, rows, i));
      stats.forEach((s, i) => lines.push(fmt(`edge ${FAIR_SHORT[i]}`, s)));

      const ok = stats.map((s, i) => ({ s, i })).filter((x) => x.s.t >= 20);
      if (ok.length < 3) {
        lines.push("  растёт ли винрейт с корзинами: мало данных (нужно ≥3 корзины с 20+ записями)");
      } else {
        let monoW = true;
        let monoR = true;
        for (let k = 1; k < ok.length; k++) {
          if (ok[k].s.w / ok[k].s.t < ok[k - 1].s.w / ok[k - 1].s.t - 0.005) monoW = false;
          if (ok[k].s.roi / ok[k].s.t < ok[k - 1].s.roi / ok[k - 1].s.t - 0.005) monoR = false;
        }
        lines.push(
          `  растёт ли винрейт с корзинами: ${ok.map((x) => winPct(x.s.w, x.s.t)).join(" → ")} ${monoW ? "✅ растёт" : "❌ не растёт — по критерию идея мертва"}; прибыль: ${ok.map((x) => roiLabel(x.s.roi, x.s.t)).join(" → ")} ${monoR ? "✅" : "—"}`,
        );
        lines.push("  (замечание: у высоких edge цена ниже, поэтому сравнивай не только винрейт, но и «запас» и прибыль)");
      }

      const cum = (from: number) =>
        stats.slice(from).reduce((a, s) => ({ w: a.w + s.w, t: a.t + s.t, roi: a.roi + s.roi, px: a.px + s.px }), { w: 0, t: 0, roi: 0, px: 0 });
      lines.push(fmt("правило «брать при edge ≥ 2%»", cum(2)));
      lines.push(fmt("правило «брать при edge ≥ 4%»", cum(3)));

      // по монетам при edge ≥ 4%
      const hi = [FAIR_EDGE_LABELS[3], FAIR_EDGE_LABELS[4]];
      const byCoin = this.collect(fv.idea).filter((r) => hi.includes(r.combo.split(" / ")[0]));
      const parts: string[] = [];
      for (const coin of INCLUDED_COINS) {
        const x = byCoin.filter((r) => r.coin === coin).reduce((a, r) => ({ w: a.w + r.w, t: a.t + r.t, roi: a.roi + r.roi }), { w: 0, t: 0, roi: 0 });
        if (x.t > 0) parts.push(`${COIN_SHORT[coin]} ${x.w}/${x.t} ${winPct(x.w, x.t)} (${roiLabel(x.roi, x.t)})`);
      }
      lines.push(`  при edge ≥ 4% по монетам: ${parts.length > 0 ? parts.join(" | ") : "пока нет данных"}`);

      // по секундам при edge ≥ 4%
      const secParts: string[] = [];
      for (const sb of MODEL_SEC_BANDS) {
        let w = 0;
        let t = 0;
        let roi = 0;
        for (const r of rows) {
          const c = r.combo.split(" / ");
          if (!hi.includes(c[0]) || c[1] !== secLabel(sb)) continue;
          w += r.w;
          t += r.t;
          roi += r.roi;
        }
        if (t > 0) secParts.push(`${secLabel(sb)} ${w}/${t} ${winPct(w, t)} (${roiLabel(roi, t)})`);
      }
      lines.push(`  при edge ≥ 4% по времени: ${secParts.length > 0 ? secParts.join(" | ") : "пока нет данных"}`);
      lines.push("");
    }

    // Сочетание с Binance-против
    lines.push("<b>══ fair2 + Binance против ══</b>");
    const rb = this.pooled("fair2b0");
    const rbTotal = rb.reduce((a, r) => a + r.t, 0);
    if (rbTotal === 0) {
      lines.push("  пока нет данных");
    } else {
      const st2 = FAIR_SHORT.map((_, i) => bucketStat("fair2b0", rb, i));
      st2.forEach((s, i) => lines.push(fmt(`edge ${FAIR_SHORT[i]}`, s)));
      const base2 = FAIR_SHORT.map((_, i) => bucketStat("fair2", this.pooled("fair2"), i));
      const cumB = (arr: { w: number; t: number; roi: number; px: number }[], from: number) =>
        arr.slice(from).reduce((a, s) => ({ w: a.w + s.w, t: a.t + s.t, roi: a.roi + s.roi, px: a.px + s.px }), { w: 0, t: 0, roi: 0, px: 0 });
      lines.push(fmt("edge ≥ 4%, Binance против", cumB(st2, 3)));
      lines.push(fmt("edge ≥ 4%, без фильтра (для сравнения)", cumB(base2, 3)));
    }
    lines.push("");

    lines.push("<b>══ Сравнение с z-моделью ══</b>");
    const m = this.sumIdea("model");
    lines.push(m.t > 0 ? `  «Модель цены» (сетка z): ${m.w}/${m.t} (${winPct(m.w, m.t)}), прибыль ${roiLabel(m.roi, m.t)}` : "  «Модель цены»: пока нет данных");
    lines.push("");
    lines.push("Прибыль — средний результат на $1 ставки (без комиссий). Одна ситуация может попасть в несколько корзин по мере того, как идёт время.");
    return lines.join("\n");
  }

  // ───── v3.1: отчёты по признакам (режим, заражение, траектория) ─────

  /** Строки по набору признаков: для всех профилей вместе и для каждого профиля, плюс правила «что если не входить». */
  private featReportLines(defNames: string[]): string[] {
    const lines: string[] = [];
    for (const name of defNames) {
      const def = FEATURE_DEFS.find((d) => d.name === name);
      if (!def) continue;
      lines.push(`<b>══ ${def.name} ══</b>`);
      const prefixes = ["all", ...BOT_PROFILES.map((p) => `bot${p.id}`)];
      for (const prefix of prefixes) {
        const buckets: Stat[] = def.labels.map((l) => this.featProf[`${prefix}|${def.name}|${l}`] ?? { w: 0, t: 0, roi: 0 });
        const tot = buckets.reduce((a, s) => ({ w: a.w + s.w, t: a.t + s.t, roi: a.roi + s.roi }), { w: 0, t: 0, roi: 0 });
        if (tot.t === 0) {
          if (prefix === "all") lines.push("  пока нет данных");
          continue;
        }
        if (prefix !== "all" && tot.t < 20) continue;
        lines.push(
          `<b>── ${prefix === "all" ? "Все профили вместе" : `Профиль ${prefix.slice(3)}`} (входов ${tot.t}, ${winPct(tot.w, tot.t)}, прибыль ${roiLabel(tot.roi, tot.t)}) ──</b>`,
        );
        def.labels.forEach((l, i) => {
          const s = buckets[i];
          if (s.t === 0) return;
          lines.push(`  ${l}: ${s.w}/${s.t} (${winPct(s.w, s.t)}), прибыль ${roiLabel(s.roi, s.t)}${s.t < 10 ? " (мало данных)" : ""}`);
        });
        for (const rule of FEAT_RULES[name] ?? []) {
          const ln = cutLine(buckets, rule.remove, rule.label);
          if (ln) lines.push(ln);
        }
      }
      lines.push("");
    }
    return lines;
  }

  /** v3.1 / C: режим волатильности, часы суток и кластеры одновременных проигрышей. */
  buildRegimeReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Режим волатильности и плохие периоды</b>");
    lines.push(
      "Идея: бывают периоды, когда сильные движения откатываются, и тогда проигрывают сразу все профили (один плохой час может стереть недели). Проверяем, определяются ли такие периоды признаком «текущая волатильность сильно выше обычной». Признак = реализованная σ монеты за последние 2 / 5 минут (из тиков фида) ÷ обычная секундная σ (та же, что в z). 1.0 = обычный режим. Пишется к каждому базовому входу профилей бота (накапливается с момента обновления до v3.1).",
    );
    lines.push("");
    lines.push(...this.featReportLines([F_REGIME2, F_REGIME5]));

    lines.push("<b>══ Одновременные проигрыши в одном окне ══</b>");
    const wins = Object.entries(this.lossWin)
      .map(([k, v]) => ({ close: Number(k), ...v }))
      .filter((x) => x.seen.length >= 2)
      .sort((a, b) => a.close - b.close);
    if (wins.length === 0) {
      lines.push("  пока нет данных (нужны окна, где входы были минимум у двух монет)");
    } else {
      const hist = [0, 0, 0, 0];
      const rvBy: { s: number; n: number }[] = [0, 1, 2, 3].map(() => ({ s: 0, n: 0 }));
      for (const x of wins) {
        const k = Math.min(x.lost.length, 3);
        hist[k]++;
        if (x.rvN > 0) {
          rvBy[k].s += x.rvSum / x.rvN;
          rvBy[k].n++;
        }
      }
      const names = ["ни одна монета не проиграла", "проиграла 1 монета", "проиграли 2 монеты", "проиграли 3+ монет"];
      lines.push(`  окон, где входы были у 2+ монет: ${wins.length}`);
      hist.forEach((n, i) => {
        lines.push(`  ${names[i]}: ${n}${rvBy[i].n > 0 ? ` (средняя σ2м/обычная на входах: ${(rvBy[i].s / rvBy[i].n).toFixed(2)})` : ""}`);
      });
      lines.push("  (если в окнах «проиграли 2-3+ монет» режим заметно выше, чем в окнах «ни одна», плохие периоды определяются волатильностью)");
      const bad = wins.filter((x) => x.lost.length >= 2).slice(-10).reverse();
      if (bad.length > 0) {
        lines.push("  Последние окна с 2+ проигрышами:");
        for (const x of bad) {
          const t = new Date(x.close).toISOString().slice(5, 16).replace("T", " ");
          lines.push(
            `    ${t} UTC: проиграли ${x.lost.map((c) => COIN_SHORT[c] ?? c).join(", ")} из ${x.seen.map((c) => COIN_SHORT[c] ?? c).join(", ")}${x.rvN > 0 ? `, σ2м/обычная ${(x.rvSum / x.rvN).toFixed(2)}` : ""}`,
          );
        }
      }
    }
    lines.push("");

    lines.push("<b>══ Час суток UTC (все профили вместе) ══</b>");
    let anyH = false;
    HOUR_LABELS.forEach((h) => {
      const s = this.featAgg[`${F_HOUR}|${h}`];
      if (!s || s.t < 5) return;
      anyH = true;
      lines.push(`  ${h}:00 → ${s.w}/${s.t} (${winPct(s.w, s.t)}), прибыль ${roiLabel(s.roi, s.t)}${s.t >= 8 && s.roi / s.t < -0.1 ? " ⚠️ плохой час" : ""}`);
    });
    if (!anyH) lines.push("  пока недостаточно данных (нужно ≥5 входов в час)");
    lines.push("");
    lines.push("Новости парсер не видит: если плохие окна не объясняются ни волатильностью, ни часом, сверь их время (список выше) с календарём событий вручную.");
    lines.push("Прибыль — средний результат на $1 ставки (без комиссий). «Все профили вместе» считает одну и ту же ситуацию несколько раз (по числу согласных профилей).");
    return lines.join("\n");
  }

  /** v3.1 / E */
  buildContagionReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Заражение между монетами</b>");
    lines.push(
      `Монеты ходят вместе. Для каждого входа считаем, у скольких соседних монет в том же окне токен нашей стороны уже сыпется: строго = был ≥${CONTAGION_PEAK.toFixed(2)}, а сейчас <${CONTAGION_DROP_TO.toFixed(2)}; мягко = был ≥${CONTAGION_SOFT_PEAK.toFixed(2)} и упал на ${CONTAGION_SOFT_DROP.toFixed(2)}+. Провал идеи: если винрейт при «1+» не хуже, чем при «0».`,
    );
    lines.push("");
    lines.push(...this.featReportLines([F_CONTAGION, F_CONTAGION_SOFT]));
    lines.push("Прибыль — средний результат на $1 ставки (без комиссий).");
    return lines.join("\n");
  }

  /** v3.1 / F */
  buildTrajectoryReport(): string {
    const lines: string[] = [];
    lines.push("<b>📊 Траектория токена перед входом</b>");
    lines.push(
      `Токен попадает в коридор сверху (падал с 0.99) или снизу (рос с 0.85) — это разные события. Считаем ask сейчас − ask ${TRAJ_LOOKBACK_SEC}с назад (по истории котировок парсера). Гипотеза: «падает» хуже; если окажется наоборот — это тоже находка.`,
    );
    lines.push("");
    lines.push(...this.featReportLines([F_TRAJ]));
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
    lines.push("Берётся самый ранний базовый вход профиля (без фильтра Binance) на каждую сторону рынка. Держим до резолва, цена входа = живой ask.");
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
        `<b>Настоящих ситуаций (без дублей): ${sitPair.t}</b> — обе выиграли ${sitPair.w} (${winPct(sitPair.w, sitPair.t)}), одна проиграла ${oneLostN}, обе проиграли ${bothLostN}. BTC ${sitB.w}/${sitB.t}, ETH ${sitE.w}/${sitE.t}.`,
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

  // ───── v3.2 «Темка»: отчёт ─────

  private tget(mode: string, idx: string | number, coin: string, side: string, tf = 5): TStat | undefined {
    return this.temkaAgg[`${temkaKeyMode(mode, tf)}|${idx}|${coin}|${side}`];
  }

  /** Знак «надёжно»: ✅ если нижняя граница 95% интервала средней прибыли выше нуля, ❌ если верхняя ниже нуля. */
  private tFlag(s: TStat): string {
    if (s.f < TEMKA_MIN_FILLS) return "";
    const mean = s.roiT / s.f;
    const se = Math.sqrt(Math.max(0, s.roiT2 / s.f - mean * mean) / s.f);
    return mean - 1.96 * se > 0 ? "✅" : mean + 1.96 * se < 0 ? "❌" : "";
  }

  private temkaLine(label: string, mode: string, coin: string, side: string, tf: number): string {
    const h = this.tget(mode, "h", coin, side, tf);
    if (!h || h.n === 0) return `  ${label}: пока нет данных`;
    const isCt = mode.startsWith("ct");
    if (h.f === 0) return `  ${label}: ордеров ${h.n}, ни один не исполнился`;
    const parts: string[] = [];
    TEMKA_TP_DELTAS.forEach((d, i) => {
      const s = this.tget(mode, i, coin, side, tf);
      if (!s || s.f === 0) return;
      parts.push(`+${Math.round(d * 100)}¢ ${winPct(s.hit, s.f)} → ${roiLabel(s.roiT, s.f)}${this.tFlag(s)}`);
    });
    let head: string;
    if (isCt) {
      // у контртренда «ордер» создаётся только по сигналу: для частоты сравниваем с числом рынков (ордеров «по рынку»)
      const mk = this.tget("m", "h", coin, side, tf);
      head = `сработал ${h.f} раз${mk && mk.n > 0 ? ` на ${mk.n} рынков (${winPct(h.f, mk.n)})` : ""}, цена ${(h.px / h.f).toFixed(3)}, без TP ${winPct(h.w, h.f)} → ${roiLabel(h.roiH, h.f)}${h.f < 10 ? " (мало данных)" : ""}`;
    } else {
      head = `вошли ${h.f}/${h.n} (${winPct(h.f, h.n)}), цена ${(h.px / h.f).toFixed(3)}, без TP ${winPct(h.w, h.f)} → ${roiLabel(h.roiH, h.f)}${h.f < 10 ? " (мало данных)" : ""}`;
    }
    return `  ${label}: ${head} | TP: ${parts.join(" | ")}`;
  }

  buildTemkaReport(tf = 5): string {
    const modes = TEMKA_MODES_BY_TF[tf] ?? TEMKA_MODES_BY_TF[5];
    const lines: string[] = [];
    const modeLabel = (id: string): string => modes.find((m) => m.id === id)?.label ?? id;
    const waits = TEMKA_LIMIT_WAITS[tf] ?? TEMKA_LIMIT_WAITS[5];
    lines.push(`<b>📊 Темка (${tf}-минутки): покупка в начале окна, продажа по тейк-профиту</b>`);
    lines.push(
      `В первые ${TEMKA_OPEN_MAX_SEC}с каждого ${tf}-минутного окна «покупаем» и Up, и Down: либо по рынку по живому ask, либо лимиткой на уровнях ${TEMKA_LIMIT_LEVELS.map((l) => l.toFixed(2)).join(" / ")} (лимитка ждёт ${waits.join(" или ")}с от открытия окна, пока ask не опустится до уровня; цена входа = уровень). Сразу ставим «продажу» на +${TEMKA_TP_DELTAS.map((d) => Math.round(d * 100)).join(" / +")} центов от входа (при входе 0.50 это ${TEMKA_TP_DELTAS.map((d) => (0.5 + d).toFixed(2)).join(" / ")}). Не достигли цели до закрытия окна — держим до официального резолва.`,
    );
    lines.push(
      `«Контртренд»: через ${TEMKA_CT_MIN_ELAPSED_SEC}с после открытия (и не позже чем за ${TEMKA_CT_MIN_SEC_TO_CLOSE}с до закрытия), если монета за ${TEMKA_CT_LOOKBACK_SEC}с резко ушла ПРОТИВ стороны (≥ ${TEMKA_CT_ZS.join(" или ")}σ) и токен этой стороны стоит не дороже ${TEMKA_CT_LEVELS.map((l) => l.toFixed(2)).join(" / ")}, покупаем его ради отскока. Один вход на сторону и режим.`,
    );
    lines.push(
      `Исполнение считается только по живому стакану (REST) с объёмом от ${TEMKA_MIN_SIZE} акций на лучшем уровне: покупка — когда ask не выше уровня, продажа — когда bid не ниже цели (по цене цели, то есть без везения). Рынки, замеченные позже ${TEMKA_OPEN_MAX_SEC}с после открытия, пропускаются. Комиссии не учтены (у входов по рынку они могут быть, у лимиток обычно нет — проверь тарифы на этих рынках). Очередь в стакане не учтена, поэтому исполнение лимиток оптимистично.`,
    );
    lines.push(
      "Как читать строку: «вошли» — сколько ордеров исполнилось; «без TP» — винрейт и прибыль, если просто держать до резолва; «+3¢ 64% → +2.1%» — цель +3 цента достигнута в 64% входов, итоговая прибыль на $1 с учётом тех, кто не дошёл и ушёл на резолв. ✅ — прибыль значимо выше нуля (95%), ❌ — значимо ниже.",
    );
    const mAll = this.tget("m", "h", "*", "*", tf);
    lines.push(`Рынков ${tf}м с входом по рынку: ${mAll ? mAll.n / 2 : 0}. ${tf === 5 ? "Команда для 15-минуток: «лаб темка 15»." : "Команда для 5-минуток: «лаб темка»."}`);
    lines.push("");

    lines.push("<b>══ Общая статистика (все монеты, Up и Down вместе) ══</b>");
    for (const m of modes) lines.push(this.temkaLine(m.label, m.id, "*", "*", tf));
    lines.push("");

    const allCombos: { mode: string; i: number; s: TStat; mean: number }[] = [];
    for (const m of modes) {
      TEMKA_TP_DELTAS.forEach((_, i) => {
        const s = this.tget(m.id, i, "*", "*", tf);
        if (s && s.f >= TEMKA_MIN_FILLS) allCombos.push({ mode: m.id, i, s, mean: s.roiT / s.f });
      });
    }
    lines.push(`<b>══ Лучшие 10 комбинаций вход + цель (минимум ${TEMKA_MIN_FILLS} входов) ══</b>`);
    if (allCombos.length === 0) {
      lines.push(`  пока недостаточно данных (нужно ≥${TEMKA_MIN_FILLS} входов на комбинацию)`);
    } else {
      allCombos
        .sort((a, b) => b.mean - a.mean)
        .slice(0, 10)
        .forEach((c, k) => {
          const h = this.tget(c.mode, "h", "*", "*", tf) as TStat;
          lines.push(
            `  ${k + 1}. ${modeLabel(c.mode)} → цель +${Math.round(TEMKA_TP_DELTAS[c.i] * 100)}¢: вошли ${c.s.f}, цель достигнута ${winPct(c.s.hit, c.s.f)}${c.s.hit > 0 ? ` (в среднем за ${(c.s.hitSec / c.s.hit).toFixed(0)}с)` : ""}, прибыль ${roiLabel(c.s.roiT, c.s.f)} ${this.tFlag(c.s)} | без TP ${roiLabel(c.s.roiH, c.s.f)}, цена входа ${(h.px / Math.max(h.f, 1)).toFixed(3)}`,
          );
        });
      const worst = [...allCombos].sort((a, b) => a.mean - b.mean).slice(0, 3).filter((c) => c.mean < 0);
      if (worst.length > 0) {
        lines.push("  Худшие:");
        for (const c of worst) {
          lines.push(`     ${modeLabel(c.mode)} → +${Math.round(TEMKA_TP_DELTAS[c.i] * 100)}¢: вошли ${c.s.f}, цель ${winPct(c.s.hit, c.s.f)}, прибыль ${roiLabel(c.s.roiT, c.s.f)} ${this.tFlag(c.s)}`);
        }
      }
    }
    lines.push("");

    lines.push("<b>══ Up против Down (все монеты) ══</b>");
    for (const m of modes) {
      lines.push(this.temkaLine(`${m.label} — Up`, m.id, "*", "Up", tf));
      lines.push(this.temkaLine(`${m.label} — Down`, m.id, "*", "Down", tf));
    }
    const diffs: { mode: string; i: number; z: number; pu: number; pd: number; fu: number; fd: number }[] = [];
    for (const m of modes) {
      TEMKA_TP_DELTAS.forEach((_, i) => {
        const u = this.tget(m.id, i, "*", "Up", tf);
        const d = this.tget(m.id, i, "*", "Down", tf);
        if (!u || !d || u.f < TEMKA_MIN_FILLS || d.f < TEMKA_MIN_FILLS) return;
        const pu = u.hit / u.f;
        const pd = d.hit / d.f;
        const pp = (u.hit + d.hit) / (u.f + d.f);
        const se = Math.sqrt(pp * (1 - pp) * (1 / u.f + 1 / d.f));
        if (se <= 0) return;
        diffs.push({ mode: m.id, i, z: (pu - pd) / se, pu, pd, fu: u.f, fd: d.f });
      });
    }
    lines.push("  Есть ли закономерность по стороне (разница в доле достигнутых целей, |z| ≥ 2):");
    const sig = diffs.filter((x) => Math.abs(x.z) >= 2).sort((a, b) => Math.abs(b.z) - Math.abs(a.z)).slice(0, 8);
    if (diffs.length === 0) {
      lines.push("     пока мало данных (нужно ≥30 входов и по Up, и по Down)");
    } else if (sig.length === 0) {
      lines.push(`     нет: из ${diffs.length} сравнений значимой разницы между Up и Down нет`);
    } else {
      for (const x of sig) {
        lines.push(
          `     ${modeLabel(x.mode)}, цель +${Math.round(TEMKA_TP_DELTAS[x.i] * 100)}¢: Up ${winPct(Math.round(x.pu * x.fu), x.fu)} (${x.fu}) против Down ${winPct(Math.round(x.pd * x.fd), x.fd)} (${x.fd}), z=${x.z.toFixed(1)}`,
        );
      }
      lines.push(`     (из ${diffs.length} сравнений около ${Math.max(1, Math.round(diffs.length * 0.05))} значимых ожидается чисто случайно, поэтому верь только устойчивой картине по многим соседним строкам)`);
    }
    lines.push("");

    lines.push("<b>══ По монетам (Up и Down вместе) ══</b>");
    for (const coin of INCLUDED_COINS) {
      const h0 = this.tget("m", "h", coin, "*", tf);
      lines.push(`<b>── ${coin}${h0 ? ` (рынков с входом по рынку: ${h0.n / 2})` : ""} ──</b>`);
      for (const m of modes) lines.push(this.temkaLine(m.label, m.id, coin, "*", tf));
      const cc: { mode: string; i: number; s: TStat; mean: number }[] = [];
      for (const m of modes) {
        TEMKA_TP_DELTAS.forEach((_, i) => {
          const s = this.tget(m.id, i, coin, "*", tf);
          if (s && s.f >= TEMKA_MIN_FILLS_COIN) cc.push({ mode: m.id, i, s, mean: s.roiT / s.f });
        });
      }
      const best = cc.sort((a, b) => b.mean - a.mean)[0];
      lines.push(
        best
          ? `  Лучшая комбинация: ${modeLabel(best.mode)} → +${Math.round(TEMKA_TP_DELTAS[best.i] * 100)}¢: вошли ${best.s.f}, цель ${winPct(best.s.hit, best.s.f)}, прибыль ${roiLabel(best.s.roiT, best.s.f)}`
          : `  Лучшая комбинация: пока мало данных (нужно ≥${TEMKA_MIN_FILLS_COIN} входов)`,
      );
      lines.push("");
    }

    lines.push("Идеи для следующих проверок: стоп-лосс и выход по времени, вход по направлению монеты (после движения), вход не только в начале окна.");
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
        return isBase(idea) ? `Профиль ${idea.slice(3)} (бот)` : idea;
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
      `Моделирование ${BOT_PROFILES.length} профилей бота (без фильтра бирж): ${RULES_TEXT}, вход по живому ask, потолок покупки как в боте, держим до резолва.`,
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
    lines.push("(считаются сигналы за весь рынок до закрытия, то есть задним числом: в бот это перенести нельзя; для момента входа есть «лаб согласие»)");
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
      `Рынков обработано: ${this.resolvedCount} (увидено всего: ${this.marketsSeenCount}), ждут резолва: ${this.pending.size}. Правила подсчёта: ${RULES_TEXT}.`,
    );
    lines.push("");

    lines.push("<b>── Реальный бот (подробно: «лаб бот») ──</b>");
    for (const idea of Object.keys(LIVE_VARIANTS)) {
      const s = this.sumIdea(idea);
      lines.push(`  ${idea}: ${s.w}/${s.t} ${winPct(s.w, s.t)}, прибыль на $1 ${roiLabel(s.roi, s.t)}`);
    }
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

    {
      lines.push("<b>── Темка (подробно: «лаб темка» и «лаб темка 15») ──</b>");
      for (const tf of TEMKA_TIMEFRAMES) {
        const th = this.tget("m", "h", "*", "*", tf);
        const t3 = this.tget("m", 1, "*", "*", tf);
        lines.push(
          th && th.f > 0 && t3 && t3.f > 0
            ? `  ${tf}м, вход по рынку на открытии: ${th.f} входов, цель +3¢ достигнута в ${winPct(t3.hit, t3.f)}, прибыль на $1 ${roiLabel(t3.roiT, t3.f)} (без TP ${roiLabel(th.roiH, th.f)})`
            : `  ${tf}м: пока нет данных`,
        );
      }
      lines.push("");
    }
    lines.push("<b>── Новое в v3.1 (по всей сетке, подробно в отдельных командах) ──</b>");
    const v31: [string, string][] = [
      ["model", "Модель без фильтра"],
      ["modelb0", "Модель + Binance против"],
      ["modelbS", "   сильно против"],
      ["modelc0", "Модель + Coinbase против"],
      ["modelk2", "Модель + обе биржи против"],
      ["fair1", "Справедливая цена σ60 (все корзины)"],
      ["fair2", "Справедливая цена σ120 (все корзины)"],
    ];
    for (const [idea, label] of v31) {
      const s = this.sumIdea(idea);
      lines.push(`  ${label}: ${s.t > 0 ? `${s.w}/${s.t} ${winPct(s.w, s.t)}, прибыль на $1 ${roiLabel(s.roi, s.t)}` : "пока нет данных"}`);
    }
    lines.push('  ("лаб модель бинанс", "лаб биржи", "лаб справедливая", "лаб режим", "лаб зараза", "лаб траектория")');
    lines.push("");

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

    lines.push("<b>── Профили бота (моделирование, без фильтра бирж) ──</b>");
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
    lines.push(`  опрос бирж: Binance ${binance.okCount}/${binance.okCount + binance.failCount}, Coinbase ${coinbase.okCount}/${coinbase.okCount + coinbase.failCount} удачных`);
    lines.push("");
    lines.push('Все команды: "лаб помощь"');
    return lines.join("\n");
  }

  buildHelp(): string {
    return [
      "<b>🧪 LAB v3.6 — команды</b>",
      "лаб — сводка по всем идеям",
      "лаб бот — как торговал бы реальный бот (итог, 24 часа, 7 дней, в долларах)",
      "лаб бинанс — профили бота с фильтром Binance по корзинам (каждая корзина — отдельный бот)",
      "лаб биржи — то же для Coinbase и «две биржи сразу» (обе против / одна / ни одной)",
      "лаб профили — профили бота и согласие профилей",
      "лаб модель — «Модель цены» (где выгодно покупать вне 0.97-0.98)",
      "лаб модель бинанс — вся сетка модели с Binance против: лесенка, доза-эффект (чуть/сильно против), дешёвые цены, Coinbase, две биржи",
      "лаб темка — 5-минутки: покупка в начале окна (по рынку, лимитками 0.50-0.35, контртренд) и продажа по +2…+10 центов; общая стата, Up/Down, по монетам",
      "лаб темка 15 — то же для 15-минуток",
      "лаб справедливая — p = Φ(d/(σ√t)), корзины edge = p − ask, растёт ли винрейт",
      "лаб режим — режим волатильности, часы суток, кластеры одновременных проигрышей",
      "лаб зараза — просевшие токены у соседних монет",
      "лаб траектория — токен падает или растёт перед входом",
      "лаб запас — идея «Запас» (z вместо порога)",
      "лаб дешево — «Недооценка» (токен дешевле 0.96)",
      "лаб паника — «Паника без разворота»",
      "лаб импульс — «Импульс последних секунд»",
      "лаб арб — «Арбитраж Up+Down»",
      "лаб монеты — подтверждение другими монетами",
      "лаб пила — фильтр «Пила» (пересечения цены открытия)",
      "лаб пара — «Пара BTC+ETH» (покупка обеих, когда сработали оба триггера)",
      "лаб лоси — вскрытие проигрышей: чем они отличаются от побед",
      "лаб согласие — винрейт в зависимости от числа профилей с сигналом",
      "лаб стоп — стоп-лосс: насколько проседает цена после входа",
      "лаб биржи все — Binance, Coinbase, Bybit, OKX, Kraken, Bitget: каждая отдельно по корзинам и вместе (сколько бирж против / подтверждают); можно «лаб биржи все 3» (дней)",
      "лаб страница — страница-разбор файлом: все идеи и ячейки, чем победы отличаются от поражений (можно: «лаб страница 24ч», «3д», «30д», «все», «15», «btc»)",
      "лаб торговля — состояние реальной торговли LAB; «лаб торговля выкл» — аварийно выключить, «лаб торговля вкл» — включить",
      "лаб помощь — этот список",
    ].join("\n");
  }

  start(): void {
    this.loadState();
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => this.checkResolutions(), 30 * 1000);
    setInterval(() => this.sampleArb(), ARB_SAMPLE_MS);
    setInterval(() => this.sampleTape(), TAPE_MS);
    void compressOldDays(LAB_DATA_DIR);
    setInterval(() => void compressOldDays(LAB_DATA_DIR), 60 * 60 * 1000);
    setInterval(() => this.saveState(), AUTOSAVE_INTERVAL_MS);
    setInterval(() => {
      console.log(
        `--- статус: апдейтов ${this.updateCount}, рынков обработано ${this.resolvedCount}, ждут резолва ${this.pending.size}, проверок входов ${this.verifyConfirmed + this.verifyRejected}, Binance ${binance.okCount}/${binance.failCount}, Coinbase ${coinbase.okCount}/${coinbase.failCount}, ${EXTRA_EXCHANGES.map((x) => `${x.label} ${(x.feed as any).okCount}/${(x.feed as any).failCount}`).join(", ")} ---`,
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

/** Telegram (parse_mode HTML) не принимает «голые» < > &: оставляем только теги <b>, <i>, <u>, <s>, <code>, <pre>, остальное экранируем. */
function escapeTelegramHtml(text: string): string {
  const parts = text.split(/(<\/?(?:b|i|u|s|code|pre)>)/);
  return parts
    .map((part, i) => (i % 2 === 1 ? part : part.replace(/&(?!(?:amp|lt|gt|quot);)/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")))
    .join("");
}

async function sendReportToTelegram(
  telegram: ReturnType<typeof createTelegramNotifier>,
  report: string,
): Promise<void> {
  const chunks = splitReportIntoChunks(report);
  for (let i = 0; i < chunks.length; i++) {
    const text = escapeTelegramHtml(i === 0 ? chunks[i] : `<b>...продолжение (${i + 1}/${chunks.length})</b>\n\n${chunks[i]}`);
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

// ───── v3.3: страница-разбор ─────

async function sendDocumentToTelegram(botToken: string, chatId: string, filename: string, content: string, caption: string): Promise<boolean> {
  try {
    const form = new FormData();
    form.append("chat_id", chatId);
    form.append("caption", caption.slice(0, 1000));
    form.append("document", new Blob([content], { type: "text/html" }), filename);
    const resp = await fetch(`https://api.telegram.org/bot${botToken}/sendDocument`, { method: "POST", body: form });
    if (!resp.ok) {
      console.error("[page] Telegram отказал:", resp.status, (await resp.text()).slice(0, 300));
      return false;
    }
    return true;
  } catch (err) {
    console.error("[page] ошибка отправки файла:", (err as Error).message);
    return false;
  }
}

const PAGE_COIN_WORDS: Record<string, string> = {
  btc: "Bitcoin", биткоин: "Bitcoin", eth: "Ethereum", эфир: "Ethereum", sol: "Solana", солана: "Solana",
  xrp: "XRP", doge: "Dogecoin", дож: "Dogecoin",
};

/** «лаб страница [24ч|3д|7д|30д|все] [5|15] [btc|eth|sol|xrp|doge]» */
async function handlePageCommand(
  botToken: string,
  chatId: string,
  telegram: ReturnType<typeof createTelegramNotifier>,
  text: string,
): Promise<void> {
  const args = text.replace(/^(лаб страница|\/lab_page)/, "").trim().split(" ").filter(Boolean);
  let days: number | null = 7;
  let label = "7 дней";
  let wm: number | undefined;
  let coin: string | undefined;
  for (const a of args) {
    let m: RegExpMatchArray | null;
    if (a === "все" || a === "всё") {
      days = null;
      label = "всё время";
    } else if ((m = a.match(/^(\d+)(д|дн|d)$/))) {
      days = Number(m[1]);
      label = `${days} дн.`;
    } else if ((m = a.match(/^(\d+)(ч|h)$/))) {
      days = Number(m[1]) / 24;
      label = `${m[1]} ч`;
    } else if ((m = a.match(/^(5|15)(м|m)?$/))) {
      wm = Number(m[1]);
    } else if (PAGE_COIN_WORDS[a]) {
      coin = PAGE_COIN_WORDS[a];
    }
  }
  await telegram?.send("⏳ Собираю страницу-разбор… (на большом периоде это может занять до минуты)");
  const t0 = Date.now();
  try {
    const sinceMs = days === null ? 0 : Date.now() - days * DAY_MS;
    const { html, data } = await buildPage(LAB_DATA_DIR, { sinceMs, periodLabel: label, coin, wm });
    const meta = data.meta as { markets: number; trades: number; rowsEmbedded: number; sampled: boolean };
    if (meta.trades === 0) {
      await telegram?.send("Пока нет записанных сделок за этот период. Данные для страницы копятся с запуска версии v3.3 — подожди, пока рынки отрезолвятся.");
      return;
    }
    const name = `lab-${new Date().toISOString().slice(0, 16).replace(/[:T]/g, "-")}.html`;
    const cap = `LAB: страница-разбор (${label}${wm ? `, ${wm}м` : ""}${coin ? `, ${COIN_SHORT[coin]}` : ""}). Рынков ${meta.markets}, сделок ${meta.trades}${meta.sampled ? `, в странице выборка ${meta.rowsEmbedded}` : ""}. Открой файл в браузере.`;
    const ok = await sendDocumentToTelegram(botToken, chatId, name, html, cap);
    if (!ok) await telegram?.send("Не удалось отправить файл (возможно, больше 50 МБ). Попробуй короче период: «лаб страница 24ч» или по монете.");
    else console.log(`[page] готово за ${((Date.now() - t0) / 1000).toFixed(1)}с, ${(html.length / 1048576).toFixed(1)} МБ`);
  } catch (err) {
    console.error("[page] ошибка сборки:", (err as Error).message);
    await telegram?.send(`Ошибка сборки страницы: ${(err as Error).message}`);
  }
}

const COMMANDS: Record<string, string[]> = {
  summary: ["лаб", "/lab"],
  temka: ["лаб темка", "лаб темка 5", "/lab_temka"],
  temka15: ["лаб темка 15", "/lab_temka15"],
  live: ["лаб бот", "/lab_bot"],
  margin: ["лаб запас", "/lab_margin"],
  cheap: ["лаб дешево", "/lab_cheap"],
  panic: ["лаб паника", "/lab_panic"],
  impulse: ["лаб импульс", "/lab_impulse"],
  arb: ["лаб арб", "/lab_arb"],
  confirm: ["лаб монеты", "/lab_coins"],
  profiles: ["лаб профили", "/lab_profiles"],
  model: ["лаб модель", "/lab_model"],
  modelbin: ["лаб модель бинанс", "/lab_modelbin"],
  saw: ["лаб пила", "/lab_saw"],
  pair: ["лаб пара", "/lab_pair"],
  binance: ["лаб бинанс", "/lab_binance"],
  exchanges: ["лаб биржи", "/lab_exchanges"],
  fair: ["лаб справедливая", "лаб фер", "/lab_fair"],
  regime: ["лаб режим", "/lab_regime"],
  contagion: ["лаб зараза", "/lab_contagion"],
  trajectory: ["лаб траектория", "/lab_traj"],
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

        if (text === "лаб страница" || text.startsWith("лаб страница ") || text === "/lab_page" || text.startsWith("/lab_page ")) {
          console.log(`[telegram] команда: "${msg.text}" → страница`);
          void handlePageCommand(botToken, chatId, telegram, text);
          continue;
        }

        if (text === "лаб торговля" || text.startsWith("лаб торговля ") || text === "/lab_trade") {
          const arg = text.replace(/^(лаб торговля|\/lab_trade)/, "").trim();
          if (arg === "выкл" || arg === "стоп") {
            lab.liveEnabled = false;
            await telegram?.send("⏸ LAB: реальная торговля ВЫКЛЮЧЕНА. Новые сигналы только пишутся «как бы купил». Уже купленные позиции остаются до резолва.");
          } else if (arg === "вкл") {
            if (!lab.hasClob()) await telegram?.send("LAB: нет подключения к бирже (задай LAB_TRADE=on и ключи в переменных парсера и перезапусти).");
            else {
              lab.liveEnabled = true;
              await telegram?.send("🟢 LAB: реальная торговля ВКЛЮЧЕНА.");
            }
          } else {
            await telegram?.send(escapeTelegramHtml(lab.liveStatusText() + '\nУправление: "лаб торговля вкл" / "лаб торговля выкл"'));
          }
          continue;
        }

        if (text === "лаб биржи все" || text.startsWith("лаб биржи все ") || text === "/lab_allex") {
          console.log(`[telegram] команда: "${msg.text}" → все биржи`);
          const arg = text.replace(/^(лаб биржи все|\/lab_allex)/, "").trim();
          const dm = arg.match(/^(\d+)(д|ч)?$/);
          const days = arg === "все" ? null : dm ? (dm[2] === "ч" ? Number(dm[1]) / 24 : Number(dm[1])) : 7;
          void (async () => {
            try {
              const rep = await buildExchangeSummary(LAB_DATA_DIR, days === null ? 0 : Date.now() - days * DAY_MS, days === null ? "всё время" : `${arg || "7"}${dm && dm[2] === "ч" ? " ч" : " дн."}`);
              await sendReportToTelegram(telegram, rep);
            } catch (err) {
              await telegram?.send(`Ошибка отчёта по биржам: ${(err as Error).message}`);
            }
          })();
          continue;
        }

        const cmd = Object.keys(COMMANDS).find((k) => COMMANDS[k].includes(text));
        if (!cmd) continue;
        console.log(`[telegram] команда: "${msg.text}" → ${cmd}`);

        let report: string;
        switch (cmd) {
          case "summary":
            report = lab.buildSummaryReport();
            break;
          case "live":
            report = lab.buildLiveReport();
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
          case "modelbin":
            report = lab.buildModelBinReport();
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
          case "exchanges":
            report = lab.buildExchangesReport();
            break;
          case "temka":
            report = lab.buildTemkaReport(5);
            break;
          case "temka15":
            report = lab.buildTemkaReport(15);
            break;
          case "fair":
            report = lab.buildFairReport();
            break;
          case "regime":
            report = lab.buildRegimeReport();
            break;
          case "contagion":
            report = lab.buildContagionReport();
            break;
          case "trajectory":
            report = lab.buildTrajectoryReport();
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
  console.log(`Файлы данных: ${STORAGE_DIR}${STORAGE_DIR === process.cwd() ? " (рабочая папка — при пересоздании контейнера пропадут!)" : " (постоянный диск)"}`);
  console.log("=== LAB v3.5: парсер идей запущен (не торгует; каждый сигнал записывается без лимитов, фильтр бирж с ожиданием) ===");
  console.log(
    "Идеи: запас (z), недооценка, паника без разворота, импульс, арбитраж, подтверждение монетами, согласие профилей (7 профилей), модель цены (+ Binance/Coinbase по всей сетке), справедливая цена, «темка» (скальп от начала окна), режим волатильности, заражение, траектория токена, пила, пара BTC+ETH, Binance/Coinbase по корзинам, лоси, стоп, «лаб бот».",
  );

  btcPriceFeed.start();
  ethPriceFeed.start();
  solPriceFeed.start();
  xrpPriceFeed.start();
  dogePriceFeed.start();
  volTracker.start();
  binance.start();
  coinbase.start();
  for (const x of EXTRA_EXCHANGES) x.feed.start();

  const logger = createLogger(false);
  const botToken = process.env.LAB_TELEGRAM_BOT_TOKEN ?? process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.LAB_TELEGRAM_CHAT_ID ?? process.env.TELEGRAM_CHAT_ID;
  const telegram = createTelegramNotifier(botToken, chatId, logger);

  const lab = new ResearchLab();
  if (telegram) lab.onPaperText = (text) => void telegram.send(escapeTelegramHtml(text));

  if (LIVE_TRADE_ENV) {
    const missing = ["PRIVATE_KEY", "FUNDER_ADDRESS"].filter((k) => !process.env[k] && !(k === "FUNDER_ADDRESS" && process.env.PROFILE_ADDRESS));
    if (missing.length > 0) {
      const t = `🔴 LAB: LAB_TRADE=on, но не заданы переменные: ${missing.join(", ")}. Торговля НЕ включена, работает «как бы купил».`;
      console.log(t);
      lab.onPaperText?.(t);
    } else {
      try {
        const apiKey = process.env.CLOB_API_KEY;
        const apiSecret = process.env.CLOB_API_SECRET;
        const apiPassphrase = process.env.CLOB_API_PASSPHRASE;
        const clob = await ClobService.init(
          {
            host: process.env.CLOB_HOST ?? "https://clob.polymarket.com",
            rpcUrl: process.env.RPC_URL,
            chainId: Number(process.env.CHAIN_ID ?? "137"),
            privateKey: process.env.PRIVATE_KEY!,
            signatureType: Number(process.env.SIGNATURE_TYPE ?? "1"),
            funderAddress: process.env.FUNDER_ADDRESS ?? process.env.PROFILE_ADDRESS,
            apiCreds: apiKey && apiSecret && apiPassphrase ? { key: apiKey, secret: apiSecret, passphrase: apiPassphrase } : undefined,
          },
          logger,
        );
        lab.setClob(clob);
        lab.liveEnabled = true;
        const t = `🟢 LAB: РЕАЛЬНАЯ торговля включена. ${Object.entries(PAPER_TRADE).map(([k, v]) => `${k} $${v.size} (потолок ${v.cap}, мин. bid ${v.minBid})`).join("; ")}. Лимиты: ${LIVE_MAX_PER_WINDOW} в окне, ${LIVE_MAX_OPEN} открытых, ${LIVE_QUOTA_PER_HOUR}/час. Выключить: «лаб торговля выкл».`;
        console.log(t);
        lab.onPaperText?.(t);
      } catch (err) {
        const t = `🔴 LAB: не удалось подключиться к бирже (${(err as Error).message}). Торговля НЕ включена, работает «как бы купил».`;
        console.log(t);
        lab.onPaperText?.(t);
      }
    }
  }
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
