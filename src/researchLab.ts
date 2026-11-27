/**
 * LAB v3.9 — исследовательский парсер идей. По умолчанию НЕ торгует; с LAB_TRADE=on реально покупает стратегии из TRADE_CFGS (bot7b0, fair2, fair85, model93) через clob.ts, каждая независимо от остальных. Ставки: «лаб объем».
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
import * as http from "http";
import * as os from "os";
import { spawn } from "child_process";
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
import { setPolyboltCreds } from "./polyboltFeed.js";
import { LabTest } from "./labTest.js";
import { PostWatcher } from "./labPost.js";
import { LabLim } from "./labLim.js";
import { appendMarketRecord, compressOldDays, buildPage, shrinkPage, buildExchangeSummary, setPathCells, MarketRec, RawTrade, TapeRow } from "./labAnalytics.js";

// ─────────────────────────── Общие настройки ───────────────────────────

// Где хранить файлы: постоянный диск Railway (RAILWAY_VOLUME_MOUNT_PATH) или папка из LAB_DATA_PATH, иначе рабочая папка.
const STORAGE_DIR = process.env.LAB_DATA_PATH || process.env.RAILWAY_VOLUME_MOUNT_PATH || process.cwd();
const STATE_FILE = path.resolve(STORAGE_DIR, "research-lab-state.json");
const LAB_TAPE_DIR = path.resolve(STORAGE_DIR, "lab-tape"); // v6: ленты рынков БЕЗ сделок (для поиска лимиток на прострелы); рядом с lab-data
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
interface TradeCfg {
  name: string; // короткое имя для команд и сообщений («лаб объем fair2 6»)
  idea: string;
  combo?: string; // если задано — торгуем только эту ячейку идеи
  label: string;
  size: number; // ставка в $ (меняется командой «лаб объем»)
  sizeRefPrice: number; // цена, от которой считаем число акций (нижняя граница коридора)
  cap: number; // потолок цены покупки
  minBid?: number; // не входить, если живой bid ниже
  /** Дополнительное условие входа (проверяется один раз, в момент первого срабатывания ячейки — как считает страница). */
  filter?: (e: LabEntry, tb60: number | null, x?: { tb30: number | null; clo: number | null; jmp?: number | null; rdd?: number | null; b0?: number | null; mono?: number | null; chi?: number | null; crg?: number | null }) => boolean;
  needTb60?: boolean; // условию нужен показатель «bid токена: вход − 60с назад»
  needTb30?: boolean; // v5.7: …«bid токена: вход − 30с назад»
  needPath?: boolean; // v6: …«токен до входа: самый резкий скачок вверх (jmp)» и «откат от своего максимума (rdd)» — по ленте токена с начала окна
  group?: string; // v6: общая группа (команды «лаб тс вкл/выкл kol», «лаб акций kol 5» действуют на всю группу)
  needClo?: boolean; // v5.7: …«свеча до входа: минимум монеты, bps» (≤ 0)
  shares?: number; // v4.0: если задано — всегда покупаем столько акций (ставка в $ тогда не используется)
  on: boolean; // v4.0: стратегия торгует (команда «лаб тс вкл/выкл»); выключенная только пишется в статистику
  goalP: number; // v4.0: дневная цель по прибыли, $ (0 = выкл): набрала за день (UTC) — до конца дня не покупает
  goalL: number; // v4.0: дневной стоп по убытку, $ (0 = выкл)
  goalW?: number; // v11: дневная цель — N прибыльных закрытых сделок за день (UTC), потом до 00:00 UTC не покупает (0/не задано = выкл)
  stopPct?: number; // v5.3: реальный стоп — продаём по рынку, когда bid ≤ цена входа × (1 − stopPct)
  fromDay?: string; // v4.8: начать торговать с этого дня (UTC, «2026-11-07»); до этого только пишет статистику
  untilDay?: string; // v7: торговать ДО конца предыдущего дня (UTC): с 00:00 этого дня только пишет статистику
  groupOne?: boolean; // v12: «одна сделка на рынок и сторону» на ВЕСЬ коллектив (как на странице): если на этот рынок/сторону уже купила другая ТС группы — эта считается виртуально (в её цель идёт, денег не тратит)
  paper?: boolean; // v11: бумажный режим — сделки только виртуально («LAB БЫ»), реальных денег нет; включается «лаб бумага <имя> вкл»
  entryPx?: number; // v14: «особый вход» как на странице: сигнал по условиям ТС, а покупаем, только когда bid токена дойдёт до (entryPx − спред сигнала); цена входа на странице = entryPx
  exact?: boolean; // v7: число акций = ставка ÷ реальная цена входа (для ТС с широким коридором цены); иначе от sizeRefPrice
}
const KOL_FROM = "2026-11-15"; // v6: коллектив начинает торговать с этого дня (UTC)
const KOL_UNTIL = "2026-11-17"; // v7: старый коллектив торгует до конца 2026-11-16 (UTC), дальше его заменяет «Набор 15» (k19)
const notStarted = (c: TradeCfg): boolean => c.fromDay !== undefined && Date.now() < Date.parse(`${c.fromDay}T00:00:00Z`);
const ended = (c: TradeCfg): boolean => c.untilDay !== undefined && Date.now() >= Date.parse(`${c.untilDay}T00:00:00Z`);
// ══ v7: помощники для фильтров «Набора 15» (считаются, как на странице: сначала округление значения до тех же знаков, потом сравнение)
const R1 = (v: number): number => Math.round(v * 10) / 10;
const R2 = (v: number): number => Math.round(v * 100) / 100;
const R3 = (v: number): number => Math.round(v * 1000) / 1000;
/** Биржа ÷ поток (во сколько раз биржа прошла от открытия относительно потока); меньше 0 — против. Нужен поток в нашу сторону > 0. */
const exRatioOf = (e: LabEntry, v: number | null | undefined): number | null => (e.pm != null && e.pm > 0 && v != null ? v / e.pm : null);
/** Корзина 0-4 («0 против, 1 <50%, 2 50-80%, 3 80-120%, 4 >120%») */
const bucketIdx = (r: number): number => (r < 0 ? 0 : r < 0.5 ? 1 : r < 0.8 ? 2 : r < 1.2 ? 3 : 4);
/** «Бирж против»: сколько из доступных бирж (Binance, Coinbase, Bybit, OKX, Kraken, Bitget) ушли против нашей стороны */
const nagOf = (e: LabEntry): number | null => {
  if (!(e.pm != null && e.pm > 0)) return null;
  const vals: Array<number | null | undefined> = [e.bin, e.cb, ...Object.values(e.xs ?? {})];
  let n = 0;
  for (const v of vals) if (v != null && bucketIdx(v / e.pm) === 0) n++;
  return n;
};
const inR = (v: number | null | undefined, lo: number | null, hi: number | null): boolean =>
  v != null && (lo === null || v >= lo - 1e-9) && (hi === null || v <= hi + 1e-9);
// v14: потолок цены для «особого входа»: страница считает вход ровно по цене входа, живой ask в момент касания может быть выше — разрешаем на LAB_EN_SLIP центов (по умолчанию 1¢) выше
const EN_SLIP = Number(process.env.LAB_EN_SLIP ?? "1");
const EN_CAP = Math.min(0.99, Math.round((0.85 + EN_SLIP / 100) * 100) / 100);
// v20: проверка «цена дошла» раз в секунду в ТОРГОВЛЕ (2 замера подряд на уровне) — дополнительно к проверке по ленте раз в 5с; кто сработает раньше, тот и покупает. Выключить: LAB_EN_1S=0
const EN_1S = (process.env.LAB_EN_1S ?? "1") !== "0";
const enCapOf = (p: number): number => Math.min(0.99, Math.round((p + EN_SLIP / 100) * 100) / 100); // v19: потолок для ТС с другим «особым входом»
const TRADE_CFGS: TradeCfg[] = [
  // ══ v11: КОЛЛЕКТИВ POJ (5 ТС, группа poj), только ETH. ВСЕ прежние ТС удалены. Ставка $1.1, дневная цель — 1 прибыльная сделка (goalW), старт 2026-11-22 00:00 UTC (= 07:00 Бангкок). Общий дневной стоп — TOTAL_STOP.
  {
    name: "p1", group: "poj", idea: "fair1", combo: "edge <0 / 180-240с / 0.60-0.70", label: "Коллектив POJ №1: СЦ σ60с, edge<0, 180-240с, ask 0.60-0.70, только ETH",
    size: 1.1, on: true, goalP: 0, goalL: 0, goalW: 1, groupOne: true, fromDay: "2026-11-22", sizeRefPrice: 0.6, cap: 0.7,
    filter: (e) => e.coin === "Ethereum",
  },
  {
    name: "p2", group: "poj", idea: "model", combo: "сила 0.5-1 / 120-180с / 0.70-0.80", label: "Коллектив POJ №2: Модель цены, сила 0.5-1, 120-180с, ask 0.70-0.80, только ETH",
    size: 1.1, on: true, goalP: 0, goalL: 0, goalW: 1, groupOne: true, fromDay: "2026-11-22", sizeRefPrice: 0.7, cap: 0.8,
    filter: (e) => e.coin === "Ethereum",
  },
  {
    name: "p3", group: "poj", idea: "model", combo: "сила 2-3 / 120-180с / 0.70-0.80", label: "Коллектив POJ №3: Модель цены, сила 2-3, 120-180с, ask 0.70-0.80, только ETH",
    size: 1.1, on: true, goalP: 0, goalL: 0, goalW: 1, groupOne: true, fromDay: "2026-11-22", sizeRefPrice: 0.7, cap: 0.8,
    filter: (e) => e.coin === "Ethereum",
  },
  {
    name: "p4", group: "poj", idea: "fair2", combo: "edge <0 / 180-240с / 0.80-0.85", label: "Коллектив POJ №4: СЦ σ120с, edge<0, 180-240с, ask 0.80-0.85, только ETH",
    size: 1.1, on: true, goalP: 0, goalL: 0, goalW: 1, groupOne: true, fromDay: "2026-11-22", sizeRefPrice: 0.8, cap: 0.85,
    filter: (e) => e.coin === "Ethereum",
  },
  {
    name: "p5", group: "poj", idea: "model", combo: "сила 1-1.5 / 180-240с / 0.85-0.90", label: "Коллектив POJ №5: Модель цены, сила 1-1.5, 180-240с, ask 0.85-0.90, только ETH",
    size: 1.1, on: true, goalP: 0, goalL: 0, goalW: 1, groupOne: true, fromDay: "2026-11-22", sizeRefPrice: 0.85, cap: 0.9,
    filter: (e) => e.coin === "Ethereum",
  },
  // ══ v14: ТС «e1» — «особый вход по 0.85» (только BTC+ETH): Справедливая цена σ120с, edge<0, 180-240с, ask 0.70-0.80; покупка, только когда цена дойдёт до 0.85 (как «Особые новые условия» на странице). Цели дня нет.
  {
    name: "e1", idea: "fair2", combo: "edge <0 / 180-240с / 0.70-0.80", label: "ТС e1: СЦ σ120с, edge<0, 180-240с, сигнал ask 0.70-0.80, ОСОБЫЙ ВХОД по 0.85, только BTC+ETH",
    size: 1.65, on: true, goalP: 0, goalL: 0, goalW: 0, paper: true, entryPx: 0.85, exact: true, sizeRefPrice: 0.85, cap: EN_CAP,
    filter: (e) => e.coin === "Bitcoin" || e.coin === "Ethereum", // v14: только BTC+ETH — на SOL/XRP биржа отвергает подпись ордера («invalid POLY_1271 signature»)
  },
  // ══ v19: два БУМАЖНЫХ замера (реальных денег нет) — найдены «перебором коллективов» на странице: проверка на новых днях. Живут рядом с e1, ничего не трогают.
  {
    name: "x1", idea: "model", combo: "сила 0.5-1 / 30-60с / 0.60-0.70", label: "Замер x1: Модель цены, сила 0.5-1, 30-60с, ask 0.60-0.70, ОСОБЫЙ ВХОД по 0.80, только ETH (бумага)",
    size: 1.1, on: true, goalP: 0, goalL: 0, goalW: 0, paper: true, entryPx: 0.80, exact: true, sizeRefPrice: 0.80, cap: enCapOf(0.80),
    filter: (e) => e.coin === "Ethereum",
  },
  {
    name: "x2", idea: "fair1", combo: "edge 0-2% / 180-240с / 0.70-0.80", label: "Замер x2: СЦ σ60с, edge 0-2%, 180-240с, ask 0.70-0.80, ОСОБЫЙ ВХОД по 0.82, только SOL (бумага)",
    size: 1.1, on: true, goalP: 0, goalL: 0, goalW: 0, paper: true, entryPx: 0.82, exact: true, sizeRefPrice: 0.82, cap: enCapOf(0.82),
    filter: (e) => e.coin === "Solana",
  },
];
/** v6: стратегии по имени, «все» или имени группы (например «kol»). */
const pickCfgs = (n: string): TradeCfg[] => (n === "все" ? TRADE_CFGS : TRADE_CFGS.filter((c) => c.name === n || (c.group !== undefined && c.group === n)));
const cfgFor = (idea: string, combo: string): TradeCfg | undefined => TRADE_CFGS.find((c) => c.idea === idea && (c.combo === undefined || c.combo === combo));
// v5.5 «Вход после дна» для ячейки fair2 «edge <0 / 60-120с / 0.80-0.85»: после сигнала НЕ покупаем сразу, а ждём — bid токена падает до low, потом возвращается до buy — и тогда покупаем по ask
// (как «Вход после дна» на странице: упала до 75, отросла до 80 — купить). Стопа нет: держим до конца. Цены меняются командой «лаб дно 75 80».
const DIP = { on: true, low: 75, buy: 80, shares: 5, minLeft: 15, spread: 3 };
const DIP_IDEA = "fair2";
const DIP_COMBO = "edge <0 / 60-120с / 0.80-0.85";
const DIP_NAME = "dip";
const dipCfg = (): TradeCfg => ({
  name: DIP_NAME, idea: DIP_IDEA, combo: DIP_COMBO,
  label: `Справедливая цена σ120с, edge <0, 60-120с, вход после дна (дно ≤${DIP.low}¢, покупка от ${DIP.buy}¢)`,
  size: DIP.shares, sizeRefPrice: 0.8, cap: Math.min(0.99, (DIP.buy + DIP.spread) / 100), shares: DIP.shares, on: DIP.on, goalP: 0, goalL: 0,
});
const MAX_STAKE = Number(process.env.LAB_MAX_STAKE ?? "50");
const MAX_SHARES = Number(process.env.LAB_MAX_SHARES ?? "100");
// v6: проверено пробой («лаб проба», 14.11.2026): биржа принимает покупку и продажу МЕНЬШЕ 5 акций (минимум 5 был только в нашем clob.ts).
// У покупки остаётся минимум $1 на сумму ордера, поэтому нижняя граница — 1 акция и не меньше ~$1.05 по цене входа. Ставки по умолчанию не менялись (5 акций).
const MIN_SHARES = 1;
const MIN_NOTIONAL = 1.05; // биржа: покупка рыночным ордером — минимум $1
const minStakeOf = (c: TradeCfg): number => Math.max(MIN_NOTIONAL, Math.ceil(MIN_SHARES * c.sizeRefPrice * 100) / 100);
const sharesOf = (c: TradeCfg, price?: number): number => (c.shares !== undefined && c.shares > 0 ? c.shares : c.size / (c.exact && price !== undefined && price > 0.05 ? price : c.sizeRefPrice));
const stakeTxt = (c: TradeCfg): string => (c.shares !== undefined && c.shares > 0 ? `${c.shares} акций` : `$${c.size}`) + (c.stopPct ? `, стоп ${Math.round(c.stopPct * 100)}%` : "");
const limitsTxt = (c: TradeCfg): string => `цель ${(c.goalW ?? 0) > 0 ? `${c.goalW} приб. сделк.` : c.goalP > 0 ? `+$${c.goalP}` : "выкл"}, стоп ${c.goalL > 0 ? `−$${c.goalL}` : "выкл"}`;
const LIVE_CFG_FILE = path.resolve(process.env.LAB_DATA_PATH || process.env.RAILWAY_VOLUME_MOUNT_PATH || process.cwd(), "lab-live-config.json");
// v9: ОБЩИЙ дневной стоп — сумма закрытых результатов всех ТС за день (UTC) ≤ −$4.5 → никто не покупает до 00:00 UTC (env LAB_TOTAL_STOP, 0 = выкл)
const TOTAL_STOP = Number(process.env.LAB_TOTAL_STOP ?? "9");
const DAY_STATE_FILE = path.resolve(path.dirname(LIVE_CFG_FILE), "lab-day-state.json");
function loadDayState(): Map<string, { day: number; real: number; told: boolean; wins?: number; wq?: number[]; stopW?: boolean; virt?: number }> {
  const m = new Map<string, { day: number; real: number; told: boolean; wins?: number; wq?: number[]; stopW?: boolean; virt?: number }>();
  try {
    const o = JSON.parse(fs.readFileSync(DAY_STATE_FILE, "utf8")) as Record<string, { day: number; real: number; told?: boolean; wins?: number; wq?: number[]; stopW?: boolean; virt?: number }>;
    const today = Math.floor(Date.now() / (24 * 60 * 60 * 1000));
    for (const [k, v] of Object.entries(o)) if (v && v.day === today && typeof v.real === "number") m.set(k, { day: v.day, real: v.real, told: !!v.told, wins: typeof v.wins === "number" ? v.wins : 0, wq: Array.isArray(v.wq) ? v.wq : [], stopW: typeof v.stopW === "boolean" ? v.stopW : (v.wins ?? 0) > 0 && v.real > 1e-9, virt: typeof v.virt === "number" ? v.virt : 0 });
  } catch {
    /* файла нет — счётчики с нуля */
  }
  // v12 (разовая правка 2026-11-22): после перезапуска в счётчики дня попали несуществующие результаты; возвращаем настоящие значения из Telegram-сообщений (все 7 реальных сделок дня)
  try {
    const FIX = path.resolve(path.dirname(DAY_STATE_FILE), "lab-day-fix-20261122.flag");
    const today = Math.floor(Date.now() / (24 * 60 * 60 * 1000));
    if (today === Math.floor(Date.UTC(2026, 10, 22) / (24 * 60 * 60 * 1000)) && !fs.existsSync(FIX)) {
      const mk = (real: number, wq: number[], stopW: boolean) => ({ day: today, real, told: stopW, wins: 1, wq, stopW, virt: 0 });
      m.set("p1", mk(0.57, [0.57], true));
      m.set("p2", mk(-0.83, [0.38], false));
      m.set("p3", mk(0.38, [0.38], true));
      m.set("p4", mk(0.31, [0.31], true));
      m.set("p5", mk(-0.86, [0.24], false));
      fs.writeFileSync(DAY_STATE_FILE, JSON.stringify(Object.fromEntries(m)));
      fs.writeFileSync(FIX, "1");
    }
  } catch {
    /* не получилось — счётчики остаются как есть */
  }
  return m;
}
function saveDayState(m: Map<string, { day: number; real: number; told: boolean; wins?: number; wq?: number[]; stopW?: boolean; virt?: number }>): void {
  try {
    fs.writeFileSync(DAY_STATE_FILE, JSON.stringify(Object.fromEntries(m)));
  } catch (err) {
    console.log("[day-state] не удалось сохранить дневные счётчики:", (err as Error).message);
  }
}
let v3Pending = false;
// v4.4: подробный путь цены по каждой сделке записываем для ячеек, которые торгует бот (для окна сделки на странице)
// v5.0: ячейки, для которых пишем путь bid по секундам, хотя бот их не торгует (проверка стопов на реальных ценах продажи)
const EXTRA_PATH_CELLS: string[] = ["fair2|edge <0 / 60-120с / 0.80-0.85"];
setPathCells([...TRADE_CFGS.filter((c) => c.combo).map((c) => `${c.idea}|${c.combo}`), ...EXTRA_PATH_CELLS]);
// v5.2 «бумажный стоп»: для этой ячейки бот ничего не покупает, но записывает, когда стоп сработал бы и по какой цене реально продалось бы
const PAPER_STOP = { idea: "fair2", combo: "edge <0 / 60-120с / 0.80-0.85", pct: 0.28, shares: 5 };
interface PaperStopRec {
  slug: string;
  coin: string;
  side: "Up" | "Down";
  tokenId: string;
  entryTs: number;
  entry: number; // цена входа (ask)
  level: number; // уровень стопа: bid ≤ level
  hitTs?: number;
  bidHit?: number; // bid в момент срабатывания
  vwap?: number; // средняя цена продажи 5 акций по глубине стакана (REST) в момент срабатывания
  depthOk?: boolean; // хватило ли глубины на все акции
  bid2?: number | null; // bid через ~2 с
  bid5?: number | null; // bid через ~5 с
  winner?: "Up" | "Down";
}
const PAPER_STOP_LOG_MAX = 500;
/** Все заявки на покупку (bids) токена: [цена, размер] по убыванию цены. */
/** Стакан целиком (v6, для бумажных «лимиток»): bids по убыванию цены, asks по возрастанию. */
async function fetchBookFull(tokenId: string): Promise<{ bids: Array<[number, number]>; asks: Array<[number, number]> } | null> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const resp = await fetch(`${CLOB_HOST}/book?token_id=${tokenId}`, { signal: ctrl.signal });
    if (!resp.ok) return null;
    const ob: any = await resp.json();
    const conv = (arr: any[]): Array<[number, number]> => {
      const out: Array<[number, number]> = [];
      for (const o of arr ?? []) {
        const px = Number(o.price), sz = Number(o.size);
        if (Number.isFinite(px) && Number.isFinite(sz) && sz > 0) out.push([px, sz]);
      }
      return out;
    };
    const bids = conv(ob.bids).sort((a, b) => b[0] - a[0]);
    const asks = conv(ob.asks).sort((a, b) => a[0] - b[0]);
    return { bids, asks };
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}
async function fetchBids(tokenId: string): Promise<Array<[number, number]> | null> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const resp = await fetch(`${CLOB_HOST}/book?token_id=${tokenId}`, { signal: ctrl.signal });
    if (!resp.ok) return null;
    const ob: any = await resp.json();
    const out: Array<[number, number]> = [];
    for (const o of ob.bids ?? []) {
      const px = Number(o.price);
      const sz = Number(o.size);
      if (Number.isFinite(px) && Number.isFinite(sz) && sz > 0) out.push([px, sz]);
    }
    out.sort((a, b) => b[0] - a[0]);
    return out;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

// v8 (разовая миграция): торгует только «Набор 2» (группа k2, 18 ТС): ставка $1.1, цель дня +$0.25 у каждой, старт 2026-11-17 00:00 UTC. Все остальные ТС выключены.
// v9 (разовая миграция): в боте остался ТОЛЬКО «Набор 1007» (группа k8, s01…s08): ставка $1.1, цель дня +$0.5 у каждой, все включены.
function applyV9(): void {
  for (const c of TRADE_CFGS) { c.on = true; c.shares = undefined; c.size = 1.1; c.goalP = 0; c.goalL = 0; c.goalW = 1; c.paper = false; }
}
function loadLiveCfg(): void {
  try {
    const o = JSON.parse(fs.readFileSync(LIVE_CFG_FILE, "utf8")) as Record<string, any>;
    for (const c of TRADE_CFGS) if (typeof o[c.name] === "number" && o[c.name] >= minStakeOf(c) && o[c.name] <= MAX_STAKE) c.size = o[c.name];
    const v2 = o.__v2 as Record<string, { shares?: number | null; on?: boolean; goalP?: number; goalL?: number; goalW?: number; paper?: boolean }> | undefined;
    if (v2 && typeof v2 === "object") {
      for (const c of TRADE_CFGS) {
        const x = v2[c.name];
        if (!x) continue;
        if (typeof x.on === "boolean") c.on = x.on;
        if (x.shares === null) c.shares = undefined;
        else if (typeof x.shares === "number" && x.shares >= MIN_SHARES && x.shares <= MAX_SHARES) c.shares = x.shares;
        if (typeof x.goalP === "number" && x.goalP >= 0) c.goalP = x.goalP;
        if (typeof x.goalL === "number" && x.goalL >= 0) c.goalL = x.goalL;
        if (typeof x.paper === "boolean") c.paper = x.paper;
        if (typeof x.goalW === "number" && x.goalW >= 0) c.goalW = x.goalW;
      }
    }
    // v12: разовая смена ставки всех ТС коллектива POJ на $1.65 (дальше «лаб объем» работает как обычно)
    if (!o.__v12) {
      for (const c of TRADE_CFGS) c.size = 1.65;
      v3Pending = true;
    }
    const dp = o.__dip as Record<string, any> | undefined;
    if (dp && typeof dp === "object") {
      if (typeof dp.on === "boolean") DIP.on = dp.on;
      if (typeof dp.low === "number" && typeof dp.buy === "number" && dp.low >= 1 && dp.buy <= 98 && dp.low < dp.buy) { DIP.low = dp.low; DIP.buy = dp.buy; }
      if (typeof dp.shares === "number" && dp.shares >= MIN_SHARES && dp.shares <= MAX_SHARES) DIP.shares = dp.shares;
      if (typeof dp.minLeft === "number" && dp.minLeft >= 3 && dp.minLeft <= 120) DIP.minLeft = dp.minLeft;
      if (typeof dp.spread === "number" && dp.spread >= 0 && dp.spread <= 10) DIP.spread = dp.spread;
    }
    // v4.3 (разовая миграция): торгуют только m98 и f30, у каждой 5 акций и цель дня +$2.5; остальные выключены (включаются командой «лаб тс вкл <имя>»)
    if (!o.__v3) {
      for (const c of TRADE_CFGS) {
        const mine = c.name === "m98" || c.name === "f30";
        c.on = mine;
        if (mine) { c.shares = 5; c.goalP = 2.5; }
      }
      v3Pending = true;
    }
    // v4.8 (разовая миграция): торгуют m98 (цель +$2.5), cb4 (цель +$3) и fbg (цель +$2.5), у каждой 5 акций; cb4 и fbg стартуют с 7 ноября (UTC)
    if (!o.__v4) {
      const goals: Record<string, number> = { m98: 2.5, cb4: 3, fbg: 2.5 };
      for (const c of TRADE_CFGS) {
        const g = goals[c.name];
        c.on = g !== undefined;
        if (g !== undefined) { c.shares = 5; c.goalP = g; }
      }
      v3Pending = true;
    }
    // v5.6 (разовая миграция): cb4 — ставка $5 (без фиксированных акций), цель дня +$3, торгует с 2026-11-13 00:00 UTC (= 07:00 по Бангкоку)
    if (!o.__v5) {
      const c = TRADE_CFGS.find((x) => x.name === "cb4");
      if (c) { c.on = true; c.shares = undefined; c.size = 5; c.goalP = 3; }
      v3Pending = true;
    }
    // v6 (разовая миграция): реально торгует ТОЛЬКО коллектив (kol1-kol10, т.е. и fz2, fz3), у каждой ставка $1.1 (без фиксированных акций), без дневной цели. Остальные выключены — только пишут статистику (включить: «лаб тс вкл <имя>»). «Вход после дна» выключен.
    if (!o.__v6) {
      for (const c of TRADE_CFGS) {
        const mine = c.group === "kol";
        c.on = mine;
        if (mine) { c.shares = undefined; c.size = 1.1; c.goalP = 0; c.goalL = 0; }
      }
      DIP.on = false;
      v3Pending = true;
    }
    // v7 (разовая миграция): «Набор 15» (группа k19: ts03…ts22 + fz2, fz3, kol8) — ставка $1.1, цель дня +$1 у каждой, включены. Старый коллектив не трогаем: он сам остановится по untilDay.
    if (!o.__v7) {
      for (const c of TRADE_CFGS) {
        if (c.group !== "k19") continue;
        c.on = true; c.shares = undefined; c.size = 1.1; c.goalP = 1; c.goalL = 0;
      }
      v3Pending = true;
    }
    if (!o.__v11) {
      applyV9();
      v3Pending = true;
    }
  } catch {
    /* файла ещё нет — настройки по умолчанию */
    applyV9();
  }
  if (v3Pending) saveLiveCfg();
}
function saveLiveCfg(): void {
  try {
    const o: Record<string, any> = Object.fromEntries(TRADE_CFGS.map((c) => [c.name, c.size]));
    o.__v2 = Object.fromEntries(TRADE_CFGS.map((c) => [c.name, { shares: c.shares ?? null, on: c.on, goalP: c.goalP, goalL: c.goalL, goalW: c.goalW ?? 0, paper: c.paper === true }]));
    o.__v3 = true;
    o.__v4 = true;
    o.__v5 = true;
    o.__v6 = true;
    o.__v7 = true;
    o.__v8 = true;
    o.__v9 = true;
    o.__v10 = true;
    o.__v11 = true;
    o.__v12 = true;
    o.__dip = { ...DIP };
    fs.writeFileSync(LIVE_CFG_FILE, JSON.stringify(o));
    tgbSettingsSoon(); // v15: копия настроек в Telegram (если нет тома)
  } catch (err) {
    console.log("[live-cfg] не удалось сохранить настройки:", (err as Error).message);
  }
}
// v3.7: РЕАЛЬНАЯ торговля теми же идеями (PAPER_TRADE). Включается переменной LAB_TRADE=on (нужны PRIVATE_KEY, FUNDER_ADDRESS и т.д. как у бота).
// v3.9: каждая стратегия торгует независимо (одна позиция на рынок — только внутри стратегии); общие лимиты широкие: 12 в окне, 30 открытых, 200 в час (env LAB_MAX_PER_WINDOW / LAB_MAX_OPEN / LAB_QUOTA_PER_HOUR).
const LIVE_TRADE_ENV = (process.env.LAB_TRADE ?? "off").toLowerCase() === "on";
const LIVE_MAX_PER_WINDOW = Number(process.env.LAB_MAX_PER_WINDOW ?? "25"); // v3.9: стратегии независимы, лимит общий и широкий
const LIVE_MAX_OPEN = Number(process.env.LAB_MAX_OPEN ?? "30");
const LIVE_QUOTA_PER_HOUR = Number(process.env.LAB_QUOTA_PER_HOUR ?? "200");
const LIVE_ATTEMPTS = 3; // сколько раз пробуем купить, если ордер не исполнился (FAK без встречных заявок)
const LIVE_RETRY_DELAY_MS = 700;
const SIG_RETRIES = 6; // v14: сколько раз повторить заказ, если биржа отвергла подпись (ничего не куплено — безопасно)
const SIG_RETRY_DELAY_MS = 150;
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
// ───── v6: свечи 15м и 1ч по монете (подтверждение старшим таймфреймом) ─────
// Раз в 15с запоминаем цену каждой монеты (история ~2 ч, сохраняется в файл), по ней считаем, что делала монета на 15-минутке и часовике до входа.
const HTF_KEEP_MS = 135 * 60 * 1000;
const HTF_STEP_MS = 15 * 1000;
interface HtfFeats { m15: number | null; m60: number | null; q5: number | null; q15: number | null; q60: number | null }
class HtfTracker {
  private h: Record<string, { t: number; p: number }[]> = {};
  private readonly file = path.resolve(STORAGE_DIR, "lab-htf.json");
  load(): void {
    try {
      const j = JSON.parse(fs.readFileSync(this.file, "utf8"));
      if (j && typeof j === "object") this.h = j;
    } catch { /* нет файла — начнём с нуля */ }
  }
  save(): void {
    try {
      fs.writeFileSync(this.file + ".tmp", JSON.stringify(this.h));
      fs.renameSync(this.file + ".tmp", this.file);
    } catch { /* не критично */ }
  }
  sample(): void {
    const t = Date.now();
    for (const [coin, feed] of Object.entries(PRICE_FEEDS)) {
      const p = feed.getLatestPrice();
      if (p === null || !(p > 0)) continue;
      const a = this.h[coin] ?? (this.h[coin] = []);
      a.push({ t, p });
      const cut = t - HTF_KEEP_MS;
      while (a.length > 0 && a[0].t < cut) a.shift();
    }
  }
  private at(coin: string, ts: number): number | null {
    const a = this.h[coin];
    if (!a || a.length === 0) return null;
    let best: number | null = null;
    let bd = Infinity;
    for (let i = a.length - 1; i >= 0; i--) {
      const d = Math.abs(a[i].t - ts);
      if (d < bd) { bd = d; best = a[i].p; }
      if (a[i].t < ts - 60000) break;
    }
    return best !== null && bd <= 45000 ? best : null;
  }
  /** Движения в bps в нашу сторону: m — с начала текущей свечи до сейчас, q — вся предыдущая свеча. */
  feats(coin: string, now: number, coinNow: number, dir: 1 | -1): HtfFeats {
    const mv = (a: number | null, b: number | null): number | null =>
      a === null || b === null || b <= 0 ? null : Math.round(((a - b) / b) * 1e4 * dir * 10) / 10;
    const o5 = Math.floor(now / 300000) * 300000;
    const o15 = Math.floor(now / 900000) * 900000;
    const o60 = Math.floor(now / 3600000) * 3600000;
    const p15 = this.at(coin, o15);
    const p60 = this.at(coin, o60);
    return {
      m15: mv(coinNow, p15),
      m60: mv(coinNow, p60),
      q5: mv(this.at(coin, o5), this.at(coin, o5 - 300000)),
      q15: mv(p15, this.at(coin, o15 - 900000)),
      q60: mv(p60, this.at(coin, o60 - 3600000)),
    };
  }
}
const htfTracker = new HtfTracker();
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
  m15?: number | null; // v6: монета с начала 15-мин свечи, bps в нашу сторону
  m60?: number | null; // …с начала часа
  q5?: number | null; // …вся предыдущая 5-мин свеча
  q15?: number | null; // …предыдущая 15-мин свеча
  q60?: number | null; // …предыдущий час
  fp?: number | null; // справедливая вероятность p
  fe?: number | null; // edge = p − ask
  xs?: Record<string, number | null>; // v3.4: Bybit/OKX/Kraken/Bitget
  agreeN?: number;
  bid0?: number | null; // v3.3: bid в момент входа
  pk?: number; // v3.3: пик цены токена до входа
  minBid?: number;
  stops?: (number | null)[];
  bs10?: number | null; // v5.4: объём лучшего bid, когда bid упал на 10¢ от входа
  dp10?: number | null; // …и объём заявок в 5¢ от лучшего bid
  bs20?: number | null;
  dp20?: number | null;
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
  m15: number | null;
  m60: number | null;
  q5: number | null;
  q15: number | null;
  q60: number | null;
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
  private wicks = new Map<string, { mn: number; mx: number }>(); // v6: минимум/максимум bid токена между замерами ленты (прострелы)
  // v6: замер «после окна» (стаканы после закрытия до резолва) — только запись, торговлю не трогает
  private postWatch = new PostWatcher({
    dir: path.resolve(STORAGE_DIR, "lab-post"),
    fetchBook: (t) => fetchBook(t),
    resolveWinner: (s) => resolveWinner(s),
    coinMove: (coin, openMs) => {
      const f = PRICE_FEEDS[coin];
      if (!f) return null;
      const o = f.getPriceAt(openMs), n = f.getLatestPrice();
      return o === null || n === null || o === 0 ? null : ((n - o) / o) * 1e4;
    },
    coinMoveAt: (coin, openMs, atMs) => {
      const f = PRICE_FEEDS[coin];
      if (!f) return null;
      const o = f.getPriceAt(openMs), n = f.getPriceAt(atMs);
      return o === null || n === null || o === 0 ? null : ((n - o) / o) * 1e4;
    },
  });
  postText(days: number): string { return this.postWatch.report(days); }

  /** «лаб минимум»: бесплатная проверка — что биржа сообщает про минимальный размер ордера на живых рынках (ничего не покупает). */
  async minInfoText(): Promise<string> {
    const list = [...this.activeMarkets.values()].filter((m) => m.closeTimeMs - Date.now() > 20000).slice(0, 4);
    if (list.length === 0) return "Живых рынков сейчас нет — повтори через минуту.";
    const L: string[] = ["📏 Что биржа говорит про минимальный ордер (только чтение, ничего не покупается):"];
    for (const m of list) {
      for (const [nm, tok] of [["Up", m.upTokenId], ["Down", m.downTokenId]] as const) {
        try {
          const resp = await fetch(`${CLOB_HOST}/book?token_id=${tok}`);
          const ob: any = await resp.json();
          L.push(`${m.eventSlug} ${nm}: min_order_size=${ob.min_order_size ?? "—"} акций, tick_size=${ob.tick_size ?? "—"}, neg_risk=${ob.neg_risk ?? "—"}`);
        } catch (err) {
          L.push(`${m.eventSlug} ${nm}: не удалось прочитать (${(err as Error).message})`);
        }
      }
    }
    L.push("");
    L.push("Это то, что биржа объявляет. Принимает ли она реально меньше — проверяет «лаб проба» (покупка ~$1, платная).");
    return L.join("\n");
  }

  /** «лаб проба да»: РЕАЛЬНАЯ покупка на ~$1 (меньше 5 акций) и сразу продажа обратно — узнать, пропускает ли биржа ордера меньше минимума. */
  async probeSmallText(confirmed: boolean, wantShares: number | null, coinOnly: string | null = null): Promise<string> {
    if (!this.clob) return "Торговый клиент не подключён — проба невозможна.";
    if (!confirmed) {
      return "⚠️ «лаб проба» — РЕАЛЬНАЯ сделка на деньги: купит на ~$1 (меньше 5 акций) токен на живом рынке и сразу продаст обратно. Потеря обычно несколько центов (спред). Цель — узнать, принимает ли биржа ордер меньше 5 акций. Чтобы выполнить, напиши: лаб проба да (или «лаб проба да 2» — столько акций, от 1 до 4.9)";
    }
    const list = [...this.activeMarkets.values()].filter((m) => m.closeTimeMs - Date.now() > 100000 && (coinOnly === null || m.coin === coinOnly)); // v14: можно проверить конкретную монету («лаб проба да sol»)
    let pick: { m: CryptoUpDownMarket; tok: string; side: string; ask: number; bid: number | null } | null = null;
    for (const m of list) {
      for (const [sd, tok] of [["Up", m.upTokenId], ["Down", m.downTokenId]] as const) {
        const b = await fetchBook(tok);
        if (b && b.ask !== null && b.ask >= 0.25 && b.ask <= 0.8 && b.bid !== null && b.ask - b.bid <= 0.03 && b.askSize >= 6) { pick = { m, tok, side: sd, ask: b.ask, bid: b.bid }; break; }
      }
      if (pick) break;
    }
    if (!pick) return "Не нашёл подходящий рынок (нужен токен 25–80¢ с узким спредом и до закрытия больше 100с). Повтори через минуту.";
    const size = wantShares !== null && wantShares >= 1 && wantShares < 5 ? wantShares : Math.min(4.9, Math.max(1, Math.ceil((1.05 / pick.ask) * 100) / 100));
    const L: string[] = [`🧪 Проба: ${pick.m.eventSlug} ${pick.side}, ask ${pick.ask}, пробую купить ${size} акций (~$${(size * pick.ask).toFixed(2)})`];
    let filled = 0;
    try {
      const resp: any = await this.clob.placeLimitOrder({ tokenId: pick.tok, side: Side.BUY, price: pick.ask, size, maxSlippagePct: 0.5, maxBuyPrice: Math.min(0.99, pick.ask + 0.03), priceAtCap: true, allowBelowMin: true } as any);
      // у биржи поля перепутаны (making = доллары, taking = акции): акции определяем так же, как при обычной покупке
      const fl = resolveFillLive(Number(resp?.filledSize ?? 0), Number(resp?.filledUsdc ?? 0), pick.ask);
      filled = fl.filledSize;
      L.push(`BUY: ✅ БИРЖА ПРИНЯЛА ордер меньше 5 акций. Куплено ${filled.toFixed(4)} акций по средней ${fl.avgPrice.toFixed(3)} (сырой ответ: ${JSON.stringify(resp).slice(0, 260)})`);
    } catch (err) {
      L.push(`BUY: ОТКАЗ — ${(err as Error).message}`);
      L.push("Вывод: покупка меньше 5 акций не прошла. Если в тексте выше нет слов «not submitted» — это отказ самой биржи (наш локальный блок в clob.ts для пробы отключён).");
      return L.join("\n");
    }
    if (filled > 0) {
      // токены появляются на счёте не мгновенно (ончейн-расчёт) — продаём с повторами, пока биржа отвечает «баланс 0»
      let sold = false;
      let lastErr = "";
      for (let k = 1; k <= 8 && !sold; k++) {
        await new Promise((r) => setTimeout(r, k === 1 ? 6000 : 4000));
        const b2 = await fetchBook(pick.tok);
        const bid = b2?.bid ?? pick.bid ?? 0.01;
        try {
          const r2: any = await this.clob.placeLimitOrder({ tokenId: pick.tok, side: Side.SELL, price: bid, size: filled, maxSlippagePct: 45, allowBelowMin: true } as any);
          const f2 = resolveFillLive(Number(r2?.filledSize ?? 0), Number(r2?.filledUsdc ?? 0), bid);
          L.push(`SELL (попытка ${k}, через ~${6 + (k - 1) * 4}с после покупки): ✅ БИРЖА ПРИНЯЛА продажу меньше 5 акций. Продано ${f2.filledSize.toFixed(4)} акций по средней ${f2.avgPrice.toFixed(3)} (сырой ответ: ${JSON.stringify(r2).slice(0, 220)})`);
          sold = true;
        } catch (err) {
          lastErr = (err as Error).message;
          if (!/balance|allowance/i.test(lastErr)) break;
        }
      }
      if (!sold) {
        L.push(`SELL: ОТКАЗ — ${lastErr}`);
        L.push(`⚠️ Купленные ${filled.toFixed(2)} акций остались на счёте — продай вручную на сайте или дождись итога рынка.`);
      }
    } else {
      L.push("Покупка не заполнилась — деньги не потрачены.");
    }
    return L.join("\n");
  }

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
  private paperStops = new Map<string, PaperStopRec>(); // ключ slug|side, пока рынок не закрылся
  private paperStopLog: PaperStopRec[] = [];
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
        paperStopLog: this.paperStopLog,
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
      this.paperStopLog = data.paperStopLog ?? [];
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
      try { this.postWatch.add(market.eventSlug, market.coin, market.windowMinutes, market.closeTimeMs, market.upTokenId, market.downTokenId); } catch { /* замер best-effort */ }
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
    for (const k of this.liveSlugs) {
      const s = k.slice(k.indexOf("|") + 1);
      if (!activeSlugs.has(s) && !this.pending.has(s) && !this.liveFills.has(k)) this.liveSlugs.delete(k);
    }
    for (const k of this.liveWindowCount.keys()) if (k < now - 3600_000) this.liveWindowCount.delete(k);
    for (const k of [...this.dipWatch.keys()]) {
      const s = k.slice(0, k.lastIndexOf("|"));
      if (!activeSlugs.has(s) && !this.pending.has(s)) {
        this.dipWatch.delete(k);
        this.dipPaper.delete(k);
      }
    }
    if (this.liveEnabled && this.clob) void Promise.resolve((this.clob as any).warmMeta?.(tokenIds)).catch(() => {}); // метаданные рынков заранее — быстрый путь ордера

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
    if (hasBid && q.bid !== null) {
      const w = this.wicks.get(update.tokenId);
      if (!w) this.wicks.set(update.tokenId, { mn: q.bid, mx: q.bid });
      else { if (q.bid < w.mn) w.mn = q.bid; if (q.bid > w.mx) w.mx = q.bid; }
    }
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
    this.trackAfterEntry(side, hasBid ? (update.bestBid as number) : null, p, now, update.tokenId);
    this.paperStopCheck(market.eventSlug, side, hasBid ? (update.bestBid as number) : null, now);
    this.liveStopCheck(market, side, hasBid ? (update.bestBid as number) : null, now);
    this.dipCheck(market, side, hasBid ? (update.bestBid as number) : q.bid, q.ask, now);
    this.enCheck(market, side, hasBid ? (update.bestBid as number) : q.bid, q.ask, now); // тик: только мгновенный вход (g ≤ 0); подъём/падение — в sampleTape
    try {
      this.test.onTick(market, side, hasBid ? (update.bestBid as number) : q.bid, q.ask, now);
    } catch (err) {
      console.log("[lab-test] ошибка тика:", (err as Error).message);
    }
    try {
      this.lim.onTick(market, side, hasBid ? (update.bestBid as number) : q.bid, q.ask, now);
    } catch (err) {
      console.log("[lab-lim] ошибка тика:", (err as Error).message);
    }

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
  /** «Бумажный стоп»: если bid упал до уровня стопа — фиксируем, что реально можно было бы получить при продаже. */
  private paperStopCheck(slug: string, side: "Up" | "Down", bid: number | null, now: number): void {
    const r = this.paperStops.get(`${slug}|${side}`);
    if (!r || r.hitTs !== undefined || bid === null || bid > r.level) return;
    r.hitTs = now;
    r.bidHit = bid;
    void (async () => {
      const bids = await fetchBids(r.tokenId);
      if (bids !== null) {
        let left = PAPER_STOP.shares;
        let sum = 0;
        for (const [px, sz] of bids) {
          const t = Math.min(left, sz);
          sum += t * px;
          left -= t;
          if (left <= 1e-9) break;
        }
        r.depthOk = left <= 1e-9;
        const got = PAPER_STOP.shares - left;
        r.vwap = got > 0 ? Math.round((sum / got) * 10000) / 10000 : 0.01;
        if (!r.depthOk) r.vwap = Math.round(((sum + left * 0.01) / PAPER_STOP.shares) * 10000) / 10000;
      }
    })();
    const later = (ms: number, key: "bid2" | "bid5"): void => {
      setTimeout(() => {
        const q = this.quotes.get(r.tokenId);
        r[key] = q ? q.bid : null;
      }, ms);
    };
    later(2000, "bid2");
    later(5000, "bid5");
    console.log(`[paper-stop] ${r.coin} ${r.side} вход ${(r.entry * 100).toFixed(0)}¢ стоп ${(r.level * 100).toFixed(0)}¢ bid ${(bid * 100).toFixed(0)}¢`);
  }

  /** v5.3: реальный стоп. Если bid токена ≤ цена входа × (1 − stopPct) — проверяем стакан по REST и продаём все акции по рынку. */
  private liveStopCheck(market: CryptoUpDownMarket, side: "Up" | "Down", bid: number | null, now: number): void {
    if (bid === null || !this.clob) return;
    const slug = market.eventSlug;
    for (const [k, f] of this.liveFills) {
      if (f.slug !== slug || f.side !== side || !f.stopPct || !f.tokenId || f.stopBusy || f.stopFail) continue;
      const level = f.avg * (1 - f.stopPct);
      // след bid за последние ~6 с — для сообщения о стопе (скорость падения)
      const tr = f.trail ?? (f.trail = []);
      if (!tr.length || now - tr[tr.length - 1][0] >= 250) tr.push([now, bid]);
      while (tr.length > 1 && now - tr[0][0] > 6000) tr.shift();
      if (bid > level + 1e-9) continue;
      if (f.stopLast !== undefined && now - f.stopLast < 1500) continue;
      f.stopLast = now;
      let zAg: number | null = null;
      try { zAg = this.temkaAgainstZ(market, side, now); } catch { zAg = null; }
      void this.liveStopSell(k, level, bid, zAg);
    }
  }

  private async liveStopSell(key: string, level: number, bidWs: number, zAg: number | null = null): Promise<void> {
    const f = this.liveFills.get(key);
    if (!f || !f.tokenId || !this.clob || f.stopBusy) return;
    f.stopBusy = true;
    const name = `${COIN_SHORT[f.coin] ?? f.coin} ${f.side}`;
    try {
      // контрольный запрос стакана: не продаём по случайному «мигающему» bid из потока
      const bids = await fetchBids(f.tokenId);
      if (bids !== null && bids.length > 0 && bids[0][0] > level + 1e-9) return;
      // данные для разбора: стакан в момент стопа и как быстро падал bid
      let book = "";
      try {
        if (bids !== null && bids.length > 0) {
          const need = f.shares - (f.soldShares ?? 0);
          let left = need, sum = 0, deep = 0;
          for (const [px, sz] of bids) {
            if (px >= bids[0][0] - 0.05 - 1e-9) deep += sz;
            if (left > 1e-9) { const t = Math.min(left, sz); sum += t * px; left -= t; }
          }
          const vw = left <= 1e-9 ? sum / need : (sum + left * 0.01) / need;
          book = `; стакан: на лучшем bid ${bids[0][1].toFixed(0)} акц., в 5¢ от него ${deep.toFixed(0)}, ${need.toFixed(1)} акц. продались бы по ~${vw.toFixed(3)}${left > 1e-9 ? " (стакана не хватает)" : ""}`;
        }
        const tr = f.trail ?? [];
        const ref = tr.find((x) => Date.now() - x[0] <= 3000);
        if (ref) book += `; bid 3 с назад ${ref[1].toFixed(2)} → ${bidWs.toFixed(2)}`;
      } catch { /* необязательно */ }
      const t0 = Date.now();
      const errors: string[] = [];
      f.stopRounds = (f.stopRounds ?? 0) + 1;
      if (f.shares - (f.soldShares ?? 0) < MIN_SHARES - 1e-9) {
        f.stopFail = true;
        this.note(`⚠️ LAB СТОП [${f.idea}] ${name}: стоп сработал, но у нас ${(f.shares - (f.soldShares ?? 0)).toFixed(2)} акций — меньше минимума биржи (${MIN_SHARES}), продать нельзя. Ждём итога рынка.`);
        return;
      }
      for (let attempt = 1; attempt <= 4; attempt++) {
        const have = f.shares - (f.soldShares ?? 0);
        if (have < MIN_SHARES - 1e-9) break; // остаток меньше минимального ордера — дождёмся итога рынка
        try {
          const resp: any = await this.clob.placeLimitOrder({ tokenId: f.tokenId, side: Side.SELL, price: bidWs, size: have, maxSlippagePct: 45, allowBelowMin: true } as any);
          const { filledSize, avgPrice } = resolveFillLive(Number(resp.filledSize ?? 0), Number(resp.filledUsdc ?? 0), bidWs);
          if (filledSize > 0) {
            f.soldShares = (f.soldShares ?? 0) + filledSize;
            f.soldUsd = (f.soldUsd ?? 0) + filledSize * avgPrice;
            this.note(`🛑 LAB СТОП [${f.idea}] ${name}: продал ${filledSize.toFixed(2)} акций по ${avgPrice.toFixed(3)} (вход ${f.avg.toFixed(3)}, стоп-уровень ${level.toFixed(3)}, bid был ${bidWs.toFixed(2)}), ${Date.now() - t0} мс, попытка ${attempt}${zAg !== null ? `; монета против нас: z=${zAg.toFixed(2)}` : ""}${book}`);
            if (f.shares - f.soldShares < MIN_SHARES - 1e-9) return;
          } else errors.push(`попытка ${attempt}: без заполнения`);
        } catch (err) {
          errors.push(`попытка ${attempt}: ${(err as Error).message}`);
        }
        await new Promise((r) => setTimeout(r, 700));
      }
      if ((f.soldShares ?? 0) < f.shares - 0.1 && (f.stopRounds ?? 0) >= 3) f.stopFail = true;
      if ((f.soldShares ?? 0) < f.shares - 0.1) this.note(`⚠️ LAB СТОП [${f.idea}] ${name}: продано ${(f.soldShares ?? 0).toFixed(2)} из ${f.shares.toFixed(2)}; остальное дождётся итога рынка.${errors.length ? `\n${errors.map((x) => `• ${x}`).join("\n")}` : ""}`);
    } catch (err) {
      this.note(`⚠️ LAB СТОП [${f.idea}] ${name}: ошибка ${(err as Error).message}`);
    } finally {
      f.stopBusy = false;
    }
  }

  private paperStopSettle(slug: string, winner: "Up" | "Down"): void {
    for (const side of ["Up", "Down"] as const) {
      const r = this.paperStops.get(`${slug}|${side}`);
      if (!r) continue;
      this.paperStops.delete(`${slug}|${side}`);
      r.winner = winner;
      this.paperStopLog.push(r);
      if (this.paperStopLog.length > PAPER_STOP_LOG_MAX) this.paperStopLog.shift();
    }
  }

  paperStopText(): string {
    const L = this.paperStopLog;
    if (L.length === 0) return "🛑 Бумажный стоп: данных пока нет (ждём срабатывания ячейки fair2 edge<0 / 60-120с / 0.80-0.85).";
    const n = PAPER_STOP.shares;
    let hold = 0;
    let stopSim = 0; // как считает страница: уровень − 5¢
    let stopReal = 0; // по реальному стакану
    let hits = 0;
    let withReal = 0;
    let shallow = 0;
    let gap = 0; // насколько реальная цена хуже уровня стопа, ¢ (сумма)
    let gapN = 0;
    let hitWins = 0; // стоп сработал, а рынок потом выиграл (стоп «зря»)
    for (const r of L) {
      const win = r.winner === r.side;
      const h = (win ? 1 - r.entry : -r.entry) * n;
      hold += h;
      if (r.hitTs === undefined) { stopSim += h; stopReal += h; continue; }
      hits++;
      if (win) hitWins++;
      const sim = (Math.max(0.01, r.level - 0.05) - r.entry) * n;
      stopSim += sim;
      if (r.vwap !== undefined) {
        withReal++;
        if (r.depthOk === false) shallow++;
        stopReal += (r.vwap - r.entry) * n;
        gap += (r.level - r.vwap) * 100;
        gapN++;
      } else stopReal += sim;
    }
    const f = (x: number): string => `${x >= 0 ? "+" : "−"}$${Math.abs(x).toFixed(2)}`;
    return (
      `🛑 Бумажный стоп ${Math.round(PAPER_STOP.pct * 100)}% (fair2 edge<0 / 60-120с / 0.80-0.85, ${n} акций), сделок ${L.length}, стоп сработал в ${hits} (${((hits / L.length) * 100).toFixed(0)}%)\n` +
      `• держим до конца: ${f(hold)}\n` +
      `• со стопом по модели страницы (−5¢): ${f(stopSim)}\n` +
      `• со стопом по РЕАЛЬНОМУ стакану: ${f(stopReal)}` + (withReal < hits ? ` (реальная цена записана в ${withReal} из ${hits})` : "") + `\n` +
      (gapN > 0 ? `• реальная цена продажи в среднем хуже уровня стопа на ${(gap / gapN).toFixed(1)}¢; не хватило глубины стакана: ${shallow}\n` : "") +
      `• стоп сработал, а рынок потом выиграл: ${hitWins} из ${hits}\n` +
      (L.length < 100 ? "Данных ещё мало (надо хотя бы 100 сделок, лучше 300)." : "")
    );
  }

  private trackAfterEntry(side: "Up" | "Down", bid: number | null, p: PendingMarket, now: number, tokenId: string): void {
    if (bid === null) return;
    for (const e of p.entries) {
      if (!isBase(e.idea) || e.side !== side || e.ts >= now) continue;
      if (e.minBid === undefined || bid < e.minBid) e.minBid = bid;
      // v5.4: только для fair2 — снимок глубины стакана в момент просадки на 10 / 20 центов (разбор стопов на странице)
      if (e.idea === "fair2") {
        if (e.bs10 === undefined && bid <= e.price - 0.1 + 1e-9) this.snapDepth(e, 10, tokenId);
        if (e.bs20 === undefined && bid <= e.price - 0.2 + 1e-9) this.snapDepth(e, 20, tokenId);
      }
      if (!e.stops) e.stops = STOP_LEVELS.map(() => null);
      for (let i = 0; i < STOP_LEVELS.length; i++) {
        if (e.stops[i] === null && bid <= STOP_LEVELS[i]) e.stops[i] = bid;
      }
    }
  }

  /** v5.4: запоминает объём лучшего bid и глубину в 5¢ от него (один запрос стакана на токен в 1.5 с, только для данных страницы). */
  private depthCache = new Map<string, { t: number; p: Promise<{ bs: number; dp: number } | null> }>();
  private snapDepth(e: LabEntry, lvl: 10 | 20, tokenId: string): void {
    if (lvl === 10) { e.bs10 = null; e.dp10 = null; } else { e.bs20 = null; e.dp20 = null; }
    if (!tokenId) return;
    const now = Date.now();
    let c = this.depthCache.get(tokenId);
    if (!c || now - c.t > 1500) {
      c = {
        t: now,
        p: fetchBids(tokenId).then((b) => {
          if (!b || b.length === 0) return null;
          let dp = 0;
          for (const [px, sz] of b) if (px >= b[0][0] - 0.05 - 1e-9) dp += sz;
          return { bs: b[0][1], dp };
        }).catch(() => null),
      };
      this.depthCache.set(tokenId, c);
      if (this.depthCache.size > 200) for (const k of this.depthCache.keys()) { this.depthCache.delete(k); break; }
    }
    void c.p.then((r) => {
      if (!r) return;
      if (lvl === 10) { e.bs10 = Math.round(r.bs); e.dp10 = Math.round(r.dp); } else { e.bs20 = Math.round(r.bs); e.dp20 = Math.round(r.dp); }
    });
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
          ...htfTracker.feats(market.coin, now, coinNow, side === "Up" ? 1 : -1),
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
      try {
        this.testSigEntry = entryRec;
        this.test.onSignal(idea, combo, market, side, ask, bid, now);
      } catch (err) {
        console.log("[lab-test] ошибка сигнала:", (err as Error).message);
      } finally {
        this.testSigEntry = null;
      }
      if (idea === PAPER_STOP.idea && combo === PAPER_STOP.combo) {
        const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
        this.paperStops.set(`${market.eventSlug}|${side}`, {
          slug: market.eventSlug, coin: market.coin, side, tokenId, entryTs: now, entry: ask,
          level: Math.max(0.01, Math.round(ask * (1 - PAPER_STOP.pct) * 100) / 100),
        });
      }
      const tcfg = cfgFor(idea, combo);
      if (tcfg !== undefined) {
        // дополнительное условие входа стратегии (считается один раз, в момент первого срабатывания ячейки — так же, как в статистике страницы)
        let pass = true;
        if (tcfg.filter) {
          const tb60 = tcfg.needTb60 ? this.tokenBidDelta(p, market, side, bid, 60, now) : null;
          const ext = {
            tb30: tcfg.needTb30 ? this.tokenBidDelta(p, market, side, bid, 30, now) : null,
            ...(tcfg.needClo ? this.candleStats(p, market, side, now, entryRec.pm ?? null) : { clo: null }),
            ...(tcfg.needPath ? this.tokenPath(p, market, side, now) : {}),
          };
          pass = tcfg.filter(entryRec, tb60, ext);
        }
        const tkey = `${tcfg.name}|${market.eventSlug}|${side}`;
        const tj = this.tjEnsure(tcfg, market, side, entryRec);
        const dayBlk = pass && tcfg.on ? this.dayBlock(tcfg) : null;
        if (!pass) { this.tradeSkipped.add(tkey); tj.st = "skip"; tj.why = "не прошла фильтр ТС (монеты/условия)"; }
        else if (!tcfg.on || notStarted(tcfg) || ended(tcfg)) {
          tj.st = "skip"; tj.why = !tcfg.on ? "ТС выключена" : "ТС ещё не стартовала / уже закончила";
          /* стратегия выключена командой «лаб тс выкл» (или ещё не наступил день старта / он уже закончился): в статистику пишем, не торгуем */
          if (tcfg.on) this.tradeSkipped.add(tkey); // v7: без «LAB БЫ» сообщений для ещё не стартовавших и уже закончивших ТС
        } else if (dayBlk !== null) {
          this.tradeSkipped.add(tkey);
          tj.st = "skip"; tj.why = dayBlk;
          const ds = this.dayState(tcfg.name);
          if (!ds.told) {
            ds.told = true;
            this.note(`🎯 LAB [${tcfg.name}]: ${dayBlk}. До 00:00 UTC новых покупок нет.`);
          }
        } else if (tcfg.entryPx !== undefined && Math.abs(tcfg.entryPx - entryRec.price) >= 0.0049) {
          // v14: «особый вход» (как на странице): не покупаем сразу — ждём, пока bid дойдёт до (цена входа − спред сигнала); дальше слежение в enCheck
          const ek = `${tcfg.name}|${market.eventSlug}|${side}`;
          if (!this.enWatch.has(ek)) this.enWatch.set(ek, { cfg: tcfg, market, side, e: entryRec, done: false });
          tj.st = "watch"; tj.why = undefined;
        } else {
          // v12: общий дедуп коллектива, как на странице («одна сделка на рынок и сторону»): первая по времени ТС покупает, остальные — виртуально (их результат идёт только в их дневную цель)
          const gk = tcfg.groupOne && tcfg.group ? `${tcfg.group}|${market.eventSlug}|${side}` : null;
          if (gk !== null && this.groupTaken.has(gk)) {
            this.paperBuy(idea, market, side, entryRec, getRatioOf("b"), tcfg, "виртуально: на этот рынок и сторону уже купила другая ТС коллектива (общий дедуп, как на странице); результат идёт только в цель этой ТС");
          } else {
            if (gk !== null) this.groupTaken.add(gk);
            if (this.liveEnabled && this.clob && !tcfg.paper) void this.liveBuy(idea, market, side, entryRec, getRatioOf("b"));
            else this.paperBuy(idea, market, side, entryRec, getRatioOf("b"));
          }
        }
      }
      // v5.5: «вход после дна» — отдельный наблюдатель на тот же сигнал (спред сигнала ≤2¢, как у fair2s)
      if (idea === DIP_IDEA && combo === DIP_COMBO && DIP.on && entryRec.bid0 != null && entryRec.price - entryRec.bid0 <= 0.02 + 1e-9) {
        const dk = `${market.eventSlug}|${side}`;
        if (!this.dipWatch.has(dk)) this.dipWatch.set(dk, { market, side, e: entryRec, armed: false, lowBid: null, lowTs: 0, done: false });
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

    // ── 16. v4.6 «Движение монеты»: идеи, которые смотрят только на то, как шла монета (в нашу сторону), без привязки к цене токена ──
    // Цена токена здесь не условие (берём широко 0.55-0.985) — на странице по ней можно фильтровать. Ячейка = «паттерн / секунды до закрытия».
    if (secToClose >= 20 && secToClose <= MODEL_MAX_SEC && dirMove > 0 && ask >= 0.55 && ask <= 0.985) {
      const sb = findBand(MODEL_SEC_BANDS, secToClose);
      const vol = getVol();
      const z0 = getZ();
      if (sb && vol !== null && vol > 1e-6 && z0 !== null && z0 >= 1) {
        const startMs = Math.max(openTimeMs, now - 300_000);
        const pts: number[] = [];
        for (let t = startMs; t < now - 1000; t += 5000) {
          const px = feed.getPriceAt(t);
          if (px === null) continue;
          pts.push(((side === "Up" ? px / openP - 1 : 1 - px / openP)) * 1e4);
        }
        const dNow = dirMove * 1e4;
        pts.push(dNow);
        const n = pts.length;
        if (n >= 8) {
          let hi = -1e9, lo = 1e9, hiIdx = 0, cross = 0, lastS = 0, maxAg = 0;
          for (let i = 0; i < n; i++) {
            if (pts[i] > hi) { hi = pts[i]; hiIdx = i; }
            if (pts[i] < lo) lo = pts[i];
            const sg = Math.sign(pts[i]);
            if (sg !== 0) { if (lastS !== 0 && sg !== lastS) cross++; lastS = sg; }
            if (i >= 2) maxAg = Math.max(maxAg, pts[i - 2] - pts[i]);
          }
          hi = Math.max(hi, 0); lo = Math.min(lo, 0);
          const rg = hi - lo;
          const s10 = vol * Math.sqrt(10 / 300) * 1e4; // σ за 10 секунд, bps
          const d10 = dNow - pts[Math.max(0, n - 3)];
          const d30 = dNow - pts[Math.max(0, n - 7)];
          const secL = secLabel(sb);
          const zl = z0 < 2 ? "сила 1-2" : "сила 2+";
          // а) прямой отрыв: монета ни разу не вернулась к открытию
          if (cross === 0) emit("cdx", `без возврата к открытию · ${zl} / ${secL}`);
          // б) пробой старого максимума: сейчас на максимуме, а прошлый максимум держался больше 30с
          if (dNow >= hi - 1e-9) {
            let prev = -1e9;
            for (let i = 0; i < n - 6; i++) if (pts[i] > prev) prev = pts[i];
            if (n - 6 > 0 && dNow > prev && prev > 0) emit("cbk", `пробой максимума · ${zl} / ${secL}`);
          }
          // в) разгон: последние 10с — сильный рывок в нашу сторону, 30с тоже вверх
          if (s10 > 1e-6 && d30 > 0) {
            const z10 = d10 / s10;
            if (z10 >= 1) emit("cac", `рывок 10с ${z10 < 2 ? "1-2" : z10 < 3 ? "2-3" : "3+"}σ / ${secL}`);
          }
          // г) откат и возврат: от максимума откатывались, теперь снова растём
          if (hi > 0 && s10 > 1e-6) {
            const pb = (hi - dNow) / hi;
            if (hiIdx < n - 1 && d10 > 0 && pb >= 0.1 && pb < 0.7 && hi >= 2 * s10) emit("cpb", `откат ${pb < 0.3 ? "10-30" : "30-70"}% и возврат / ${secL}`);
          }
          // д) тело свечи: монета шла почти одним движением
          if (rg > 2 * s10 && s10 > 1e-6) {
            const body = dNow / rg;
            if (body >= 0.5) emit("cbd", `тело свечи ${body < 0.8 ? "50-80" : "80+"}% · ${zl} / ${secL}`);
          }
          // е) запас против самого большого отката: текущий отрыв ÷ самый сильный откат за 10с за последние 5 минут
          if (maxAg > 0.1) {
            const r = dNow / maxAg;
            if (r >= 2) emit("crv", `отрыв в ${r < 3 ? "2-3" : r < 5 ? "3-5" : "5+"} раза больше любого отката / ${secL}`);
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

  // v4.0: дневная цель / дневной стоп (по закрытым сделкам стратегии, день по UTC)
  // v7: дневные счётчики цели/стопа хранятся и на диске (lab-day-state.json рядом с настройками), чтобы деплой/перезапуск посреди дня НЕ обнулял «цель +$1 набрана»
  private paperBought = new Set<string>(); // v12: «ТС|рынок|сторона» — виртуальные покупки, сделанные в ЭТОМ запуске (только их результат идёт в дневные счётчики)
  private groupTaken = new Set<string>(); // v12: «группа|рынок|сторона», где коллектив уже купил
  private dayPnl: Map<string, { day: number; real: number; told: boolean; wins?: number; wq?: number[]; stopW?: boolean; virt?: number }> = loadDayState();
  private dayState(name: string): { day: number; real: number; told: boolean; wins?: number; wq?: number[]; stopW?: boolean; virt?: number } {
    const d = Math.floor(Date.now() / DAY_MS);
    let st = this.dayPnl.get(name);
    if (!st || st.day !== d) {
      st = { day: d, real: 0, told: false, wins: 0, wq: [], stopW: false };
      this.dayPnl.set(name, st);
    }
    return st;
  }
  private addDayPnl(name: string, pnl: number, virtual = false): void {
    const ds = this.dayState(name);
    ds.real += pnl;
    if (virtual) ds.virt = (ds.virt ?? 0) + pnl; // виртуальные (бумажные) сделки идут в цель ТС, но не в общий стоп дня (там только реальные деньги)
    if (pnl > 1e-9) {
      ds.wins = (ds.wins ?? 0) + 1;
      // цель «N приб. сделок сверху» (как на странице): после закрытия победы берём сумму последних N побед; если итог дня (по закрытым) не меньше этой суммы — стоп до конца дня
      const W = TRADE_CFGS.find((c) => c.name === name)?.goalW ?? 0;
      if (W > 0) {
        const wq = (ds.wq ??= []);
        wq.push(pnl);
        if (wq.length > W) wq.shift();
        if (wq.length >= W && ds.real >= wq.reduce((a, b) => a + b, 0) - 1e-9) ds.stopW = true;
      }
    }
    saveDayState(this.dayPnl);
  }
  private totalDayPnl(): number {
    let t = 0;
    for (const c of TRADE_CFGS) { const d = this.dayState(c.name); t += d.real - (d.virt ?? 0); }
    return t;
  }
  private dayBlock(c: TradeCfg): string | null {
    if (TOTAL_STOP > 0) {
      const tot = this.totalDayPnl();
      if (tot <= -TOTAL_STOP + 1e-9) return `ОБЩИЙ дневной стоп −$${TOTAL_STOP} сработал (все ТС вместе сегодня −$${Math.abs(tot).toFixed(2)}) — вся торговля остановлена`;
    }
    const st = this.dayState(c.name);
    if (c.goalP > 0 && st.real >= c.goalP - 1e-9) return `дневная цель +$${c.goalP} достигнута (сегодня ${st.real >= 0 ? "+" : "−"}$${Math.abs(st.real).toFixed(2)})`;
    if ((c.goalW ?? 0) > 0 && st.stopW) return `дневная цель: ${c.goalW} приб. сделк. сверху набрана (итог дня ${st.real >= 0 ? "+" : "−"}$${Math.abs(st.real).toFixed(2)} не меньше суммы последних ${c.goalW} побед)`;
    if (c.goalL > 0 && st.real <= -c.goalL + 1e-9) return `дневной стоп −$${c.goalL} сработал (сегодня −$${Math.abs(st.real).toFixed(2)})`;
    return null;
  }
  dayLimitsText(): string {
    const tot = this.totalDayPnl();
    const head = TOTAL_STOP > 0 ? `Общий стоп дня: −$${TOTAL_STOP}; все ТС вместе сегодня ${tot >= 0 ? "+" : "−"}$${Math.abs(tot).toFixed(2)}${tot <= -TOTAL_STOP + 1e-9 ? " — ВСЯ ТОРГОВЛЯ ОСТАНОВЛЕНА до 00:00 UTC" : ""}\n` : "";
    return head + TRADE_CFGS.map((c) => {
      const st = this.dayState(c.name);
      return `• ${c.name} ${c.paper ? "📝бумага " : ""}${c.on ? (ended(c) ? "🏁" : notStarted(c) ? "🕒" : "🟢") : "⏸"} ${stakeTxt(c)}${notStarted(c) ? `, старт ${c.fromDay} 00:00 UTC` : ""}${ended(c) ? `, закончила ${c.untilDay} 00:00 UTC (дальше только статистика)` : ""}, ${limitsTxt(c)}; сегодня закрыто ${st.real >= 0 ? "+" : "−"}$${Math.abs(st.real).toFixed(2)}${this.dayBlock(c) ? " — остановлена на день" : ""}`;
    }).join("\n");
  }

  // ───── v16: журнал сделок (методы) ─────
  private tj = new Map<string, Tj>();
  private tjDirty = false;
  private tjKey(cfg: string, slug: string, side: string): string { return `${cfg}|${slug}|${side}`; }
  private tjEnsure(cfg: TradeCfg, market: CryptoUpDownMarket, side: "Up" | "Down", e: LabEntry): Tj {
    const k = this.tjKey(cfg.name, market.eventSlug, side);
    let c = this.tj.get(k);
    if (!c) {
      c = { k, cfg: cfg.name, coin: COIN_SHORT[market.coin] ?? market.coin, side, slug: market.eventSlug, close: market.closeTimeMs, sig: e.ts, sigPx: e.price, want: cfg.entryPx, cap: cfg.entryPx !== undefined ? cfg.cap : undefined, st: "skip", why: "сигнал без действия", created: Date.now() };
      this.tj.set(k, c);
    }
    this.tjDirty = true;
    return c;
  }
  private tjSet(cfg: TradeCfg, market: CryptoUpDownMarket, side: "Up" | "Down", e: LabEntry, st: TjSt, why?: string): void {
    try { const c = this.tjEnsure(cfg, market, side, e); c.st = st; c.why = why; } catch { /* журнал best-effort */ }
  }
  private tjLoad(): void {
    try {
      const arr = JSON.parse(fs.readFileSync(TJ_FILE, "utf8")) as Tj[];
      for (const c of arr) if (c && c.k) this.tj.set(c.k, c);
    } catch { /* файла ещё нет */ }
  }
  private tjSave(): void {
    if (!this.tjDirty) return;
    try {
      const cut = Date.now() - TJ_KEEP_MS;
      let arr = [...this.tj.values()].filter((c) => c.created >= cut);
      if (arr.length > TJ_MAX) arr = arr.sort((a, b) => a.created - b.created).slice(arr.length - TJ_MAX);
      if (arr.length !== this.tj.size) { this.tj.clear(); for (const c of arr) this.tj.set(c.k, c); }
      fs.writeFileSync(TJ_FILE, JSON.stringify(arr));
      this.tjDirty = false;
    } catch (err) {
      console.log("[tj] не удалось сохранить журнал:", (err as Error).message);
    }
  }
  /** «лаб сделки [ТС] [N ч|N д] [все]» */
  tradesText(arg: string): string[] {
    const toks = arg.split(/\s+/).filter(Boolean);
    let cfg: string | null = null, hours = 24, all = false;
    for (const t of toks) {
      const m = t.match(/^(\d+)\s*(ч|час|h|д|дн|d)?$/);
      if (/^(все|all)$/.test(t)) all = true;
      else if (m) { const n = Number(m[1]); hours = m[2] && /^(д|дн|d)$/.test(m[2]) ? n * 24 : n; }
      else if (TRADE_CFGS.some((c) => c.name.toLowerCase() === t)) cfg = t;
    }
    hours = Math.max(1, Math.min(72, hours));
    return tjFormat([...this.tj.values()], { cfg, hours, all, now: Date.now() });
  }

  private paperBuy(idea: string, market: CryptoUpDownMarket, side: "Up" | "Down", e: LabEntry, ratio: number | null, cfgIn?: TradeCfg, extra?: string): void {
    const cfg = cfgIn ?? cfgFor(idea, e.combo)!;
    try {
      const tj = this.tjEnsure(cfg, market, side, e);
      tj.st = extra && extra.startsWith("виртуально") ? "virtual" : "paper";
      tj.why = tj.st === "virtual" ? "виртуально: уже купила другая ТС коллектива" : undefined;
      tj.at = e.ts; tj.px = e.price; tj.sh = sharesOf(cfg, e.price);
    } catch { /* журнал best-effort */ }
    this.paperBought.add(`${cfg.name}|${market.eventSlug}|${side}`);
    const shares = sharesOf(cfg, e.price);
    const t = new Date(e.ts).toISOString().slice(11, 23);
    const text =
      `📝 LAB БЫ КУПИЛ [${cfg.name}, ${stakeTxt(cfg)}] ${t} UTC\n` +
      `${COIN_SHORT[market.coin] ?? market.coin} ${market.windowMinutes}м ${side}, ask ${e.price.toFixed(3)}, bid ${e.bid0 != null ? e.bid0.toFixed(3) : "—"} (потолок бота ${cfg.cap})\n` +
      `до закрытия ${e.sec}с, z ${e.z != null ? e.z.toFixed(2) : "—"}, Binance ${ratio !== null ? `${(ratio * 100).toFixed(0)}%` : "—"} от потока, ~${shares.toFixed(2)} акций${extra ? `, ${extra}` : ""}\n` +
      `${market.eventSlug}`;
    console.log(text.replace(/\n/g, " | "));
    this.onPaperText?.(text);
  }

  // ───── v3.7: реальная торговля ─────

  private clob: ClobService | null = null;
  liveEnabled = false; // рантайм-переключатель («лаб торговля вкл/выкл»)
  private liveSlugs = new Set<string>(); // рынки, где уже была попытка живой покупки
  private liveFills = new Map<string, { slug: string; idea: string; coin: string; side: "Up" | "Down"; shares: number; avg: number; usd: number; tokenId?: string; stopPct?: number; stopBusy?: boolean; stopLast?: number; soldShares?: number; soldUsd?: number; stopFail?: boolean; stopRounds?: number; trail?: Array<[number, number]> }>(); // ключ: «стратегия|рынок»
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
      `Стратегии:\n${this.dayLimitsText()}\n(менять: «лаб тс», «лаб объем», «лаб акций», «лаб лимит»)\n` +
      `Куплено ${s.bought}, не удалось ${s.failed}, пропущено по лимитам ${s.skipped}; закрыто: ${s.wins} побед / ${s.losses} проигрышей, итог ${s.pnl >= 0 ? "+" : "−"}$${Math.abs(s.pnl).toFixed(2)}\n` +
      `Открыто сейчас: ${this.liveFills.size} (лимит ${LIVE_MAX_OPEN}); за последний час покупок: ${this.liveTimes.filter((t) => Date.now() - t < 3600_000).length}/${LIVE_QUOTA_PER_HOUR}`
    );
  }

  private async liveBuy(idea: string, market: CryptoUpDownMarket, side: "Up" | "Down", e: LabEntry, ratio: number | null, cfgIn?: TradeCfg, extra?: string): Promise<void> {
    const cfg = cfgIn ?? cfgFor(idea, e.combo)!;
    const slug = market.eventSlug;
    const name = `${COIN_SHORT[market.coin] ?? market.coin} ${market.windowMinutes}м ${side}`;
    const head = `[${cfg.name}, ${stakeTxt(cfg)}] ${name}, ask ${e.price.toFixed(3)}, bid ${e.bid0 != null ? e.bid0.toFixed(3) : "—"}, z ${e.z != null ? e.z.toFixed(2) : "—"}, Binance ${ratio !== null ? `${(ratio * 100).toFixed(0)}%` : "—"}, до закрытия ${e.sec}с${extra ? `, ${extra}` : ""}`;
    if (!this.clob) return;
    const lk = `${cfg.name}|${slug}`;
    if (this.liveSlugs.has(lk)) return; // эта же стратегия в этом рынке уже пробовала (другие стратегии не мешают)
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
      this.liveSlugs.add(lk);
      this.liveStats.skipped++;
      this.tjSet(cfg, market, side, e, "limit", `лимит: ${limit}`);
      this.note(`⛔ LAB НЕ КУПИЛ (лимит: ${limit})\n${head}\n${slug}`);
      return;
    }
    this.liveSlugs.add(lk);
    this.pendingLive++;
    this.liveWindowCount.set(market.closeTimeMs, (this.liveWindowCount.get(market.closeTimeMs) ?? 0) + 1);

    const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
    // для ТС со стопом берём на ~6% больше: после округления/комиссии заполнение бывает 4.9 акции, а биржа не даёт продать меньше 5 — стоп бы не сработал
    const size0 = cfg.stopPct ? Math.ceil(sharesOf(cfg, e.price) * 106) / 100 : sharesOf(cfg, e.price);
    const size = Math.max(size0, Math.ceil((MIN_NOTIONAL / Math.max(0.05, e.price)) * 100) / 100); // меньше $1 по сумме биржа не берёт
    const errors: string[] = [];
    const t0 = Date.now();
    let sigRetries = 0; // v14: биржа иногда отвергает подпись ордера («invalid POLY_1271 signature»); ничего при этом не куплено, поэтому такие отказы повторяем отдельно (не тратя обычные попытки)
    let sigFirst = "";
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
          // snapshot/priceAtCap есть только в clob.ts v7.16+; через any код компилируется и со старым clob.ts (лишние поля там игнорируются)
          const orderParams: any = {
            tokenId,
            side: Side.BUY,
            price: e.price,
            size,
            maxSlippagePct: 0.5,
            maxBuyPrice: cfg.cap,
            minBidPrice: cfg.minBid,
            snapshot: attempt === 1 && e.bid0 != null && snapAge <= 1500 ? { bid: e.bid0, ask: e.price, ageMs: snapAge } : undefined,
            priceAtCap: true,
            cleanAmount: sigRetries % 2 === 1, // v14: после отказа подписи каждая вторая попытка идёт с «чистой» суммой (до центов) — вдруг дело в округлении
            allowBelowMin: true, // v6: меньше 5 акций биржа принимает (проверено), блок только в старом clob.ts
          };
          const resp: any = await this.clob.placeLimitOrder(orderParams);
          const rawA = Number(resp.filledSize ?? 0);
          const rawB = Number(resp.filledUsdc ?? 0);
          const { filledSize, avgPrice } = resolveFillLive(rawA, rawB, e.price);
          if (filledSize <= 0) {
            errors.push(`попытка ${attempt}: биржа ответила без заполнения (сырые ${rawA}/${rawB})`);
          } else {
            const usd = filledSize * avgPrice;
            this.liveFills.set(lk, { slug, idea: cfg.name, coin: e.coin, side, shares: filledSize, avg: avgPrice, usd, tokenId, stopPct: cfg.stopPct });
            this.liveTimes.push(Date.now());
            this.liveStats.bought++;
            try { const tj = this.tjEnsure(cfg, market, side, e); tj.st = "bought"; tj.why = undefined; tj.at = Date.now(); tj.px = avgPrice; tj.sh = filledSize; tj.usd = usd; } catch { /* журнал */ }
            this.note(
              `💰 LAB КУПИЛ ${head}\n` +
                `${filledSize.toFixed(2)} акций по ${avgPrice.toFixed(4)} (≈$${usd.toFixed(2)}), попытка ${attempt}/${LIVE_ATTEMPTS}, ордер ${Date.now() - tAttempt} мс, от сигнала ${Date.now() - e.ts} мс${resp.timing ? `, путь ${resp.timing.path === "fast" ? "быстрый" : "обычный"}` : ""}${sigRetries > 0 ? `\n⚠️ прошло после ${sigRetries} отказ(ов) подписи (чистая сумма: ${sigRetries % 2 === 1 ? "да" : "нет"})` : ""}\n${slug}`,
            );
            return;
          }
        } catch (err) {
          const em = (err as Error).message ?? String(err);
          if (/signature/i.test(em) && sigRetries < SIG_RETRIES) {
            sigRetries++;
            if (!sigFirst) sigFirst = em;
            attempt--; // эта попытка не считается
            await new Promise((r) => setTimeout(r, SIG_RETRY_DELAY_MS));
            continue;
          }
          errors.push(`попытка ${attempt}: ${em}`);
        }
        if (attempt < LIVE_ATTEMPTS) await new Promise((r) => setTimeout(r, LIVE_RETRY_DELAY_MS));
      }
      if (sigRetries > 0) errors.push(`подпись отвергнута ${sigRetries} раз(а) подряд (повторял): ${sigFirst}`);
      this.liveStats.failed++;
      this.tjSet(cfg, market, side, e, "failed", errors.length ? `${errors[0].replace(/^попытка \d+: /, "").slice(0, 140)}${errors.length > 1 ? ` (+ещё ${errors.length - 1})` : ""}` : "нет ответа");
      this.liveWindowCount.set(market.closeTimeMs, Math.max(0, (this.liveWindowCount.get(market.closeTimeMs) ?? 1) - 1));
      this.note(`🚫 LAB НЕ СМОГ КУПИТЬ ${head}\nПотрачено ${Date.now() - t0} мс, причины:\n${errors.map((x) => `• ${x}`).join("\n")}\n${slug}`);
    } catch (err) {
      this.liveStats.failed++;
      this.tjSet(cfg, market, side, e, "failed", `ошибка: ${(err as Error).message.slice(0, 140)}`);
      this.note(`🚫 LAB: ошибка при покупке ${head}\n${(err as Error).message}\n${slug}`);
    } finally {
      this.pendingLive--;
    }
  }
  // v5.6 «ЛАБ ТЕСТ»: отдельные бумажные тесты ТС со стопами / входами после дна (ничего не покупает; см. labTest.ts)
  private testSigEntry: LabEntry | null = null;
  private test = new LabTest({
    fetchBids: (tokenId) => fetchBids(tokenId),
    getBid: (tokenId) => this.quotes.get(tokenId)?.bid ?? null,
    dataDir: path.dirname(LIVE_CFG_FILE),
    cfgFilter: (name) => {
      const c = TRADE_CFGS.find((x) => x.name === name);
      const e = this.testSigEntry;
      return !c || !c.filter || !e ? true : c.filter(e, null);
    },
  });
  // v6 «ЛАБ ЛИМИТКИ»: бумажные идеи про прострелы по живому стакану (ничего не покупает; см. labLim.ts)
  private lim = new LabLim({ fetchBook: (t) => fetchBookFull(t), dataDir: path.dirname(LIVE_CFG_FILE) });
  limText(arg?: string): string { return this.lim.statusText(arg); }
  limReset(): void { this.lim.reset(); }
  testText(arg?: string): string {
    return this.test.statusText(arg);
  }
  testReset(): void {
    this.test.reset();
  }
  // v5.5 «Вход после дна»: слежение за сигналами ячейки fair2 (рынок|сторона) и бумажные покупки, если реальная торговля выключена
  private dipWatch = new Map<string, { market: CryptoUpDownMarket; side: "Up" | "Down"; e: LabEntry; armed: boolean; lowBid: number | null; lowTs: number; done: boolean }>();
  private dipPaper = new Map<string, { slug: string; coin: string; side: "Up" | "Down"; price: number; shares: number }>();
  private dipCheck(market: CryptoUpDownMarket, side: "Up" | "Down", bid: number | null, ask: number | null, now: number): void {
    if (this.dipWatch.size === 0 || bid === null) return;
    const w = this.dipWatch.get(`${market.eventSlug}|${side}`);
    if (!w || w.done || !DIP.on) return;
    const left = (market.closeTimeMs - now) / 1000;
    if (left < DIP.minLeft) {
      w.done = true; // слишком поздно для покупки
      return;
    }
    if (!w.armed) {
      if (bid <= DIP.low / 100 + 1e-9) {
        w.armed = true;
        w.lowBid = bid;
        w.lowTs = now;
      }
      return;
    }
    if (bid < DIP.buy / 100 - 1e-9 || ask === null) return;
    if (ask > (DIP.buy + DIP.spread) / 100 + 1e-9) return; // спред раздут — ждём, пока ask подойдёт
    w.done = true;
    const e: LabEntry = { ...w.e, price: ask, bid0: bid, ts: now, sec: Math.round(left) };
    const cfg = dipCfg();
    const extra = `дно ${w.lowBid !== null ? w.lowBid.toFixed(2) : "—"} (через ${Math.round((w.lowTs - w.e.ts) / 1000)}с после сигнала), вернулось до ${bid.toFixed(2)}`;
    if (this.liveEnabled && this.clob) void this.liveBuy(DIP_IDEA, market, side, e, null, cfg, extra);
    else {
      this.paperBuy(DIP_IDEA, market, side, e, null, cfg, extra);
      this.dipPaper.set(`${market.eventSlug}|${side}`, { slug: market.eventSlug, coin: market.coin, side, price: ask, shares: sharesOf(cfg) });
    }
  }
  // v14 «Особый вход» (как «Особые новые условия» на странице, applyEntry): сигнал по ТС, покупка — когда цена дойдёт до entryPx.
  // Страница: P = цена входа; если P выше цены сигнала px0 — нужен подъём bid до P − spr (spr = спред сигнала, если нет данных — 0.01); если P ниже — падение bid до P − spr; если почти равна — вход сразу.
  // Вход на странице считается ровно по P; бот платит живой ask (не выше cfg.cap) — разницу видно в сообщении «LAB БЫ КУПИЛ».
  private enWatch = new Map<string, { cfg: TradeCfg; market: CryptoUpDownMarket; side: "Up" | "Down"; e: LabEntry; done: boolean; n1?: number; s1?: boolean; s2?: boolean }>();
  private enBought = new Map<string, { price: number; shares: number }>(); // «ТС|рынок|сторона» → реальная (бумажная) цена и число акций
  private enCheck(market: CryptoUpDownMarket, side: "Up" | "Down", bid: number | null, ask: number | null, now: number, sampled = false): void {
    if (this.enWatch.size === 0 || bid === null) return;
    for (const [k, w] of this.enWatch) {
      if (w.done || w.market.eventSlug !== market.eventSlug || w.side !== side) continue;
      const cfg = w.cfg;
      const P = cfg.entryPx as number;
      const px0 = w.e.price;
      const spr = w.e.bid0 != null ? Math.max(0, Math.round((px0 - w.e.bid0) * 100) / 100) : 0.01; // спред сигнала, до 0.01 (как столбец spr на странице)
      const left = (market.closeTimeMs - now) / 1000;
      if (left <= 0) { w.done = true; continue; }
      const rise = P > px0 + 0.0049;
      // как на странице: если P − спред ≤ цены сигнала (g ≤ 0) — подъёма bid не требуется, вход сразу; иначе нужен подъём bid до P − спред (а при P ниже сигнала — падение bid до P − спред)
      // v15: страница видит цену только по ленте раз в 5с (замер bid), а не по каждому тику. Поэтому «цена дошла» (подъём/падение) бот проверяет ТОЛЬКО в момент замера ленты (sampled=true),
      // иначе он покупает на мгновенный всплеск bid, которого страница не видит (и такие сделки почти всегда проигрышные). Исключение: g ≤ 0 — вход сразу, как на странице.
      const immediate = rise && P - spr - px0 <= 0;
      if (!sampled && !immediate) continue;
      const hit = rise ? (immediate || bid >= P - spr - 1e-9) : bid <= P - spr + 1e-9;
      if (!hit) continue;
      if (ask === null) { // v17: цена дошла, но у рынка нет цены продажи — раньше это было не видно
        const t0 = this.tj.get(this.tjKey(cfg.name, market.eventSlug, side));
        if (t0) { t0.nA = (t0.nA ?? 0) + 1; this.tjDirty = true; }
        continue;
      }
      if (ask > cfg.cap + 1e-9) {
        // v16: запоминаем для «лаб сделки»: цена дошла, но ask выше потолка (раньше такой пропуск не оставлял следов)
        const tjm = this.tj.get(this.tjKey(cfg.name, market.eventSlug, side));
        if (tjm) { if (tjm.miss === undefined) { tjm.miss = ask; tjm.missAt = now; } tjm.missN = (tjm.missN ?? 0) + 1; tjm.cap = cfg.cap; this.tjDirty = true; }
        continue; // ask убежал выше потолка — ждём, вдруг вернётся (как «Вход после дна»)
      }
      if (!cfg.on || notStarted(cfg) || ended(cfg)) { w.done = true; continue; }
      const blk = this.dayBlock(cfg);
      if (blk !== null) { w.done = true; continue; }
      w.done = true;
      const e2: LabEntry = { ...w.e, price: ask, bid0: bid, ts: now, sec: Math.round(left) };
      const extra = `особый вход: сигнал был на ${px0.toFixed(2)}, bid дошёл до ${bid.toFixed(2)} через ${Math.round((now - w.e.ts) / 1000)}с (на странице вход считается по ${P.toFixed(2)})`;
      if (this.liveEnabled && this.clob && !cfg.paper) void this.liveBuy(w.e.idea, market, side, e2, null, cfg, extra);
      else {
        this.enBought.set(k, { price: ask, shares: sharesOf(cfg, ask) });
        this.paperBuy(w.e.idea, market, side, e2, null, cfg, extra);
      }
    }
  }
  /** v17: «теневая» проверка входа раз в секунду: вход, если цена на уровне 2 замера подряд. НИЧЕГО НЕ ПОКУПАЕТ — только пишет в журнал «лаб сделки», чтобы сравнить с обычной проверкой раз в 5с. */
  private shadowCheck(): void {
    if (this.enWatch.size === 0) return;
    const now = Date.now();
    for (const w of this.enWatch.values()) {
      try {
        const m = w.market;
        const left = (m.closeTimeMs - now) / 1000;
        if (left <= -2) continue;
        const tj = this.tj.get(this.tjKey(w.cfg.name, m.eventSlug, w.side));
        if (!tj) continue;
        const q = this.quotes.get(w.side === "Up" ? m.upTokenId : m.downTokenId);
        const bid = q?.bid ?? null;
        const ask = q?.ask ?? null;
        tj.s1on = true;
        if (bid !== null) { tj.mb = Math.max(tj.mb ?? 0, bid); tj.lb = bid; tj.la = ask ?? undefined; tj.lt = now; }
        if (w.s1 || left <= 0 || bid === null) continue;
        const cfg = w.cfg;
        const P = cfg.entryPx as number;
        const px0 = w.e.price;
        const spr = w.e.bid0 != null ? Math.max(0, Math.round((px0 - w.e.bid0) * 100) / 100) : 0.01;
        const rise = P > px0 + 0.0049;
        const hit = rise ? (P - spr - px0 <= 0 || bid >= P - spr - 1e-9) : bid <= P - spr + 1e-9;
        if (!hit) { w.n1 = 0; continue; }
        if (ask === null) { tj.nA1 = (tj.nA1 ?? 0) + 1; w.n1 = 0; continue; }
        w.n1 = (w.n1 ?? 0) + 1;
        if (w.n1 < 2) continue;
        if (ask > cfg.cap + 1e-9) { if (tj.s1Miss === undefined) tj.s1Miss = ask; continue; }
        w.s1 = true; tj.s1 = ask; tj.s1At = now; this.tjDirty = true;
        if (EN_1S && !w.done) { // v20: настоящая покупка по проверке раз в секунду (те же условия и потолок, что в enCheck)
          w.done = true;
          if (!cfg.on || notStarted(cfg) || ended(cfg) || this.dayBlock(cfg) !== null) continue;
          const k1 = `${cfg.name}|${m.eventSlug}|${w.side}`;
          const e2: LabEntry = { ...w.e, price: ask, bid0: bid, ts: now, sec: Math.round(left) };
          const extra1 = `особый вход (проверка раз в секунду): сигнал был на ${px0.toFixed(2)}, bid дошёл до ${bid.toFixed(2)} через ${Math.round((now - w.e.ts) / 1000)}с (на странице вход считается по ${P.toFixed(2)})`;
          if (this.liveEnabled && this.clob && !cfg.paper) void this.liveBuy(w.e.idea, m, w.side, e2, null, cfg, extra1);
          else {
            this.enBought.set(k1, { price: ask, shares: sharesOf(cfg, ask) });
            this.paperBuy(w.e.idea, m, w.side, e2, null, cfg, extra1);
          }
        }
      } catch { /* журнал best-effort */ }
    }
  }
  /** v18: «теневой» вход на другом уровне цены (LAB_SHADOW_PX, по умолчанию 0.87): те же правила, что у обычной проверки (замер раз в 5с, покупка по ask, потолок как у ТС), но уровень выше. Ничего не покупает — пишет в журнал «лаб сделки». */
  private shadowLevel(market: CryptoUpDownMarket, side: "Up" | "Down", bid: number | null, ask: number | null, now: number): void {
    if (!(SHADOW_PX > 0) || this.enWatch.size === 0 || bid === null) return;
    for (const w of this.enWatch.values()) {
      try {
        if (w.s2 || w.market.eventSlug !== market.eventSlug || w.side !== side) continue;
        if ((market.closeTimeMs - now) / 1000 <= 0) continue;
        const cfg = w.cfg;
        const tj = this.tj.get(this.tjKey(cfg.name, market.eventSlug, side));
        if (!tj) continue;
        tj.s2on = true;
        const px0 = w.e.price;
        const spr = w.e.bid0 != null ? Math.max(0, Math.round((px0 - w.e.bid0) * 100) / 100) : 0.01;
        const P = SHADOW_PX;
        const rise = P > px0 + 0.0049;
        const hit = rise ? (P - spr - px0 <= 0 || bid >= P - spr - 1e-9) : bid <= P - spr + 1e-9;
        if (!hit || ask === null) continue;
        if (ask > cfg.cap + 1e-9) { if (tj.s2Miss === undefined) tj.s2Miss = ask; continue; }
        w.s2 = true; tj.s2 = ask; tj.s2At = now; this.tjDirty = true;
      } catch { /* журнал best-effort */ }
    }
  }
  dipStatusText(): string {
    return (
      `📉 Вход после дна [${DIP_NAME}] — ${DIP.on ? "🟢 включён" : "⏸ выключен"}\n` +
      `ТС: Справедливая цена σ120с, edge <0, 60-120с, ask 0.80-0.85 (спред сигнала ≤2¢). Стопа нет — держим до конца.\n` +
      `Дно: bid упал до ${DIP.low}¢ или ниже. Покупка: когда потом bid вернулся до ${DIP.buy}¢ и ask не выше ${DIP.buy + DIP.spread}¢.\n` +
      `Размер: ${DIP.shares} акций. Не покупать, если до закрытия осталось меньше ${DIP.minLeft}с.\n` +
      `Сейчас следим за ${[...this.dipWatch.values()].filter((w) => !w.done).length} сигналами.\n\n` +
      `Команды:\n` +
      `«лаб дно 75 80» — цены: дно и покупка (в центах)\n` +
      `«лаб дно вкл» / «лаб дно выкл»\n` +
      `«лаб дно акций 5» — сколько акций\n` +
      `«лаб дно мин 15» — не покупать, если до конца меньше N секунд\n` +
      `«лаб дно спред 3» — максимум ¢ над ценой покупки, который готов заплатить`
    );
  }

  private pendingLive = 0;
  private tradeSkipped = new Set<string>(); // «стратегия|рынок|сторона»: ячейка сработала, но дополнительное условие входа не выполнено

  /** «Bid токена: вход − N с назад» — как на странице: bid сейчас минус последний bid той же стороны на момент (вход − N с), не старше 12 с в ленте. */
  /** v5.7: «Свеча до входа: минимум монеты, bps» — как на странице: низшая точка движения монеты (в нашу сторону) от открытия окна до входа, ≤ 0, по ленте раз в 5с и текущему движению */
  private candleStats(p: PendingMarket, market: CryptoUpDownMarket, side: "Up" | "Down", nowMs: number, pm: number | null): { clo: number | null; chi: number | null; crg: number | null } {
    if (!p.tape || p.tape.length < 3) return { clo: null, chi: null, crg: null };
    const openMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
    const entryT = (nowMs - openMs) / 1000;
    const sg = side === "Up" ? 1 : -1;
    let lo = 0, hi = 0;
    for (const r of p.tape) {
      if ((r[0] as number) > entryT) continue;
      const v = r[5];
      if (v === null || v === undefined) continue;
      const x = sg * (v as number);
      if (x < lo) lo = x;
      if (x > hi) hi = x;
    }
    if (pm !== null) { lo = Math.min(lo, pm * 1e4); hi = Math.max(hi, pm * 1e4); }
    return { clo: Math.round(lo * 10) / 10, chi: Math.round(hi * 10) / 10, crg: Math.round((hi - lo) * 10) / 10 };
  }

  /** v6: «скачок вверх» (jmp) и «откат от максимума» (rdd) по ленте bid токена с начала окна до входа — как считает страница (шаг 5с, значения до 0.001, нужно ≥3 шагов). */
  private tokenPath(p: PendingMarket, market: CryptoUpDownMarket, side: "Up" | "Down", nowMs: number): { jmp: number | null; rdd: number | null; b0: number | null; mono: number | null } {
    const none = { jmp: null, rdd: null, b0: null, mono: null };
    if (!p.tape || p.tape.length === 0) return none;
    const openMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
    const entryT = (nowMs - openMs) / 1000;
    const col = side === "Up" ? 1 : 3;
    let prev: number | null = null, first: number | null = null, mx = -1, maxJ = 0, maxDd = 0, steps = 0, up = 0;
    for (const row of p.tape) {
      if ((row[0] as number) > entryT) break;
      const raw = row[col];
      if (raw === null || raw === undefined) continue;
      const b = raw as number;
      if (first === null) first = b;
      if (prev !== null) { steps++; if (b >= prev - 1e-9) up++; if (b - prev > maxJ) maxJ = b - prev; }
      if (b > mx) mx = b;
      if (mx - b > maxDd) maxDd = mx - b;
      prev = b;
    }
    if (steps < 3 || first === null) return none;
    return { jmp: Math.round(maxJ * 1000) / 1000, rdd: Math.round(maxDd * 1000) / 1000, b0: Math.round(first * 1000) / 1000, mono: Math.round((up / steps) * 100) / 100 };
  }

  private tokenBidDelta(p: PendingMarket, market: CryptoUpDownMarket, side: "Up" | "Down", bidNow: number | null, backSec: number, nowMs: number): number | null {
    if (bidNow === null || !p.tape || p.tape.length === 0) return null;
    const openMs = market.closeTimeMs - market.windowMinutes * 60 * 1000;
    const entryT = (nowMs - openMs) / 1000;
    if (entryT - backSec < -1) return null;
    const target = entryT - backSec;
    const col = side === "Up" ? 1 : 3;
    const tp = p.tape;
    let idx = -1;
    for (let i = tp.length - 1; i >= 0; i--) {
      if ((tp[i][0] as number) <= target) {
        idx = i;
        break;
      }
    }
    for (let i = idx; i >= 0 && target - (tp[i][0] as number) <= 12; i--) {
      const v = tp[i][col];
      if (v !== null && v !== undefined) return Math.round((bidNow - v) * 1000) / 1000;
    }
    return null;
  }

  /** Закрывает все живые позиции рынка (их может быть несколько — по одной на стратегию). Возвращает имена стратегий, у которых была покупка. */
  private liveSettle(slug: string, winner: "Up" | "Down"): Set<string> {
    const bought = new Set<string>();
    for (const [k, f] of [...this.liveFills]) {
      if (f.slug !== slug) continue;
      this.liveFills.delete(k);
      bought.add(f.idea);
      const sold = f.soldShares ?? 0;
      const rest = Math.max(0, f.shares - sold);
      const won = sold > 0 ? false : f.side === winner;
      const pnl = (f.soldUsd ?? 0) - sold * f.avg + (f.side === winner ? rest * (1 - f.avg) : -rest * f.avg);
      this.liveStats.pnl += pnl;
      this.addDayPnl(f.idea, pnl);
      { const tjf = this.tj.get(this.tjKey(f.idea, slug, f.side)); if (tjf) { tjf.pnl = pnl; tjf.won = f.side === winner; if (sold > 0) tjf.stop = true; this.tjDirty = true; } }
      if (pnl >= 0) this.liveStats.wins++;
      else this.liveStats.losses++;
      this.note(`${pnl >= 0 ? "✅" : "❌"} LAB итог [${f.idea}] ${COIN_SHORT[f.coin] ?? f.coin} ${f.side} по ${f.avg.toFixed(3)}: ${sold > 0 ? `закрыто стопом (${sold.toFixed(1)} акций)` : won ? "победа" : "проигрыш"} ${pnl >= 0 ? "+" : "−"}$${Math.abs(pnl).toFixed(2)} (всего ${this.liveStats.pnl >= 0 ? "+" : "−"}$${Math.abs(this.liveStats.pnl).toFixed(2)}) ${slug}`);
    }
    return bought;
  }

  private paperSettle(p: PendingMarket, slug: string, winner: "Up" | "Down"): void {
    const boughtNames = this.liveSettle(slug, winner);
    try { tjSettleCards(this.tj.values(), slug, winner, TRADE_CFGS); this.tjDirty = true; } catch { /* журнал */ }
    for (const k of [...this.groupTaken]) if (k.indexOf(`|${slug}|`) >= 0) this.groupTaken.delete(k);
    for (const sd of ["Up", "Down"] as const) {
      this.dipWatch.delete(`${slug}|${sd}`);
      const dp = this.dipPaper.get(`${slug}|${sd}`);
      if (dp) {
        this.dipPaper.delete(`${slug}|${sd}`);
        const won = dp.side === winner;
        const pnl = won ? dp.shares * (1 - dp.price) : -dp.shares * dp.price;
        const text = `${won ? "✅" : "❌"} LAB БЫ: итог [${DIP_NAME}] ${COIN_SHORT[dp.coin] ?? dp.coin} ${dp.side} по ${dp.price.toFixed(3)}: ${won ? "победа" : "проигрыш"} ${pnl >= 0 ? "+" : "−"}$${Math.abs(pnl).toFixed(2)}`;
        console.log(text);
        this.onPaperText?.(text);
      }
    }
    for (const k of [...this.enWatch.keys()]) if (k.indexOf(`|${slug}|`) >= 0) this.enWatch.delete(k);
    for (const e of p.entries) {
      const cfg = cfgFor(e.idea, e.combo);
      if (cfg === undefined) continue;
      if (this.tradeSkipped.delete(`${cfg.name}|${slug}|${e.side}`)) continue; // условие входа не выполнено (или дневной лимит) — стратегия там не торговала
      if (!cfg.on && !this.liveSlugs.has(`${cfg.name}|${slug}`)) continue; // стратегия выключена
      const won = e.side === winner;
      if (this.liveSlugs.has(`${cfg.name}|${slug}`)) {
        // живая попытка была: если не купили — сообщаем, сколько упустили/сэкономили
        if (!boughtNames.has(cfg.name)) {
          const sh = sharesOf(cfg);
          const pn = won ? sh * (1 - e.price) : -sh * e.price;
          this.note(`ℹ️ LAB: рынок ${slug} закрыт, мы там НЕ купили; ${won ? "упущено" : "избежали"} ${pn >= 0 ? "+" : "−"}$${Math.abs(pn).toFixed(2)}`);
        }
        continue;
      }
      // v12: результат считаем только у виртуальных сделок, реально открытых в этом запуске (после перезапуска старые записи рынка не должны попадать в счётчики дня)
      if (!this.paperBought.delete(`${cfg.name}|${slug}|${e.side}`)) continue;
      const enb = cfg.entryPx !== undefined ? this.enBought.get(`${cfg.name}|${slug}|${e.side}`) : undefined; // v14: у «особого входа» цена/акции — реальные на момент касания, а не цена сигнала
      this.enBought.delete(`${cfg.name}|${slug}|${e.side}`);
      const buyPx = enb ? enb.price : e.price;
      const shares = enb ? enb.shares : sharesOf(cfg);
      const pnl = won ? shares * (1 - buyPx) : -shares * buyPx;
      this.addDayPnl(cfg.name, pnl, true);
      { const tjp = this.tj.get(this.tjKey(cfg.name, slug, e.side)); if (tjp) { tjp.pnl = pnl; tjp.won = won; this.tjDirty = true; } }
      const text = `${won ? "✅" : "❌"} LAB БЫ: итог [${e.idea}${cfg.entryPx !== undefined ? `/${cfg.name}` : ""}] ${COIN_SHORT[e.coin] ?? e.coin} ${e.side} по ${buyPx.toFixed(3)}: ${won ? "победа" : "проигрыш"} ${pnl >= 0 ? "+" : "−"}$${Math.abs(pnl).toFixed(2)} (${slug})`;
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
      // v6: прострелы — минимум и максимум bid каждого токена за время между замерами (индексы 8-11: Up мин, Up макс, Down мин, Down макс)
      for (const [tok, q] of [[m.upTokenId, qu], [m.downTokenId, qd]] as const) {
        const w = this.wicks.get(tok);
        row.push(r3(w ? w.mn : q?.bid), r3(w ? w.mx : q?.bid));
        if (q?.bid !== null && q?.bid !== undefined) this.wicks.set(tok, { mn: q.bid, mx: q.bid });
        else this.wicks.delete(tok);
      }
      (p.tape ??= []).push(row);
      // v15: проверка «особого входа» по замеру ленты — как считает страница (bid в момент замера, раз в 5с)
      if (this.enWatch.size > 0) {
        this.shadowLevel(m, "Up", qu?.bid ?? null, qu?.ask ?? null, now);
        this.shadowLevel(m, "Down", qd?.bid ?? null, qd?.ask ?? null, now);
        this.enCheck(m, "Up", qu?.bid ?? null, qu?.ask ?? null, now, true);
        this.enCheck(m, "Down", qd?.bid ?? null, qd?.ask ?? null, now, true);
      }
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
          m15: e.m15 ?? null,
          m60: e.m60 ?? null,
          q5: e.q5 ?? null,
          q15: e.q15 ?? null,
          q60: e.q60 ?? null,
          fe: e.fe ?? null,
          xings: e.xings ?? null,
          agreeN: e.agreeN ?? null,
          c05: e.c05,
          c10: e.c10,
          hr: e.hr ?? null,
          pk: e.pk ?? null,
          bsz: e.bsz ?? null,
          asz: e.asz ?? null,
          bs10: e.bs10 ?? null,
          dp10: e.dp10 ?? null,
          bs20: e.bs20 ?? null,
          dp20: e.dp20 ?? null,
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
      if (trades.length === 0 && (p.tape?.length ?? 0) < 20) return;
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
      appendMarketRecord(trades.length === 0 ? LAB_TAPE_DIR : LAB_DATA_DIR, rec);
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
    this.paperStopSettle(slug, winner);
    try {
      this.test.onSettle(slug, winner);
    } catch (err) {
      console.log("[lab-test] ошибка итога:", (err as Error).message);
    }
    try {
      this.lim.onSettle(slug, winner);
    } catch (err) {
      console.log("[lab-lim] ошибка итога:", (err as Error).message);
    }

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
      "лаб страница — страница-разбор файлом: все идеи и ячейки, чем победы отличаются от поражений (без периода — всё время; можно: «лаб страница 24ч», «3д», «7д», «30д», «15», «btc»)",
      "лаб объем — ставки торговых стратегий LAB; «лаб объем bot7b0 6», «лаб объем fair2 5», «лаб объем все 5»",
      "лаб лимитки — бумажная проверка идей про прострелы по живому стакану: «лаб лимитки», «лаб лимитки C01», «лаб лимитки сброс»",
      "лаб сделки — журнал сделок для сверки со страницей: во сколько купили, что не купили (цена убежала / ошибка биржи), что не дошло до цены; «лаб сделки e1», «лаб сделки e1 12ч», «лаб сделки все» (с пропусками по фильтрам)",
      "лаб бэкап — копия данных и настроек в Telegram прямо сейчас (когда нет тома Railway; сама делается каждые 3 часа). Перед деплоем: «лаб бэкап»",
      "лаб минимум — бесплатно: какой минимальный размер ордера объявляет биржа",
      "лаб проба — проверка покупки меньше 5 акций (РЕАЛЬНЫЕ ~$1, спросит подтверждение); «лаб проба да sol» — то же на конкретной монете (btc/eth/sol/xrp/doge)",
      "лаб после [дней] — что происходит с токенами после закрытия окна до резолва (замер, без покупок)",
      "лаб тест — бумажные тесты ТС со стопами/входами после дна (без покупок): «лаб тест», «лаб тест T01», «лаб тест сброс»",
      "лаб дно — вход после дна (fair2): цены дна и покупки, вкл/выкл: «лаб дно 75 80»",
      "лаб бумага — ТС без реальных денег: «лаб бумага t05 вкл», «лаб бумага t05 выкл»",
      "лаб тс — стратегии LAB: список, «лаб тс выкл fair2», «лаб тс вкл cb4»",
      "лаб акций — покупать всегда N акций: «лаб акций cb4 5» (выкл — обратно в $)",
      "лаб лимит — дневная цель/стоп: «лаб лимит cb4 цель 20», «лаб лимит cb4 убыток 15», «лаб лимит все выкл»",
      "лаб торговля — состояние реальной торговли LAB; «лаб торговля выкл» — аварийно выключить, «лаб торговля вкл» — включить",
      "лаб помощь — этот список",
    ].join("\n");
  }

  start(): void {
    this.loadState();
    this.tjLoad();
    setInterval(() => this.tjSave(), 30 * 1000);
    this.postWatch.start();
    htfTracker.load();
    htfTracker.sample();
    setInterval(() => htfTracker.sample(), HTF_STEP_MS);
    setInterval(() => htfTracker.save(), 5 * 60 * 1000);
    this.refreshMarkets();
    setInterval(() => this.refreshMarkets(), MARKET_REFRESH_MS);
    setInterval(() => this.checkResolutions(), 30 * 1000);
    setInterval(() => this.sampleArb(), ARB_SAMPLE_MS);
    setInterval(() => this.sampleTape(), TAPE_MS);
    setInterval(() => this.shadowCheck(), 1000); // v17: теневая проверка входа раз в секунду (только журнал)
    void compressOldDays(LAB_DATA_DIR);
    void compressOldDays(LAB_TAPE_DIR);
    setInterval(() => void compressOldDays(LAB_DATA_DIR), 60 * 60 * 1000);
    setInterval(() => void compressOldDays(LAB_TAPE_DIR), 60 * 60 * 1000);
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

// ───── v15: резервная копия в Telegram (когда нет постоянного диска Railway) ─────
// Бот раз в LAB_TG_BACKUP_HOURS (3 ч) сжимает данные за последние LAB_TG_KEEP_DAYS (7) дней, режет на куски по 19 МБ (бот может скачать из Telegram не больше 20 МБ)
// и шлёт в чат; последнее сообщение со списком кусков закрепляется. После перезапуска (чистый диск) бот находит закреплённое сообщение, скачивает куски и разворачивает данные.
// Настройки торговли (lab-live-config.json) — отдельный маленький файл, отправляется сразу после каждого изменения. Выключить: LAB_TG_BACKUP=off. По умолчанию включено, только если нет тома (LAB_DATA_PATH / RAILWAY_VOLUME_MOUNT_PATH).
const TGB_ENABLED = (() => {
  const v = (process.env.LAB_TG_BACKUP ?? "auto").toLowerCase();
  if (v === "off" || v === "0" || v === "выкл") return false;
  if (v === "on" || v === "1" || v === "вкл") return true;
  return !process.env.LAB_DATA_PATH && !process.env.RAILWAY_VOLUME_MOUNT_PATH;
})();
const TGB_KEEP_DAYS = Math.max(1, Math.floor(Number(process.env.LAB_TG_KEEP_DAYS ?? "7")) || 7);
const TGB_HOURS = Math.max(0.25, Number(process.env.LAB_TG_BACKUP_HOURS ?? "3") || 3);
const TGB_PART = 19 * 1024 * 1024;
const TGB_MAX_PARTS = 12;
const TGB_TAG = "LABBACKUP1 ";
const TGB_DIRS = ["lab-data", "lab-tape", "lab-post"];
interface TgbRef { i: string; m: number } // file_id и номер сообщения
interface TgbManifest { t: number; s: TgbRef | null; p: TgbRef[]; d: number; b: number }
const tgb: { token: string; chat: string; man: TgbManifest | null; manMsg: number | null; busy: boolean; setHash: string; setTimer: NodeJS.Timeout | null } = {
  token: "", chat: "", man: null, manMsg: null, busy: false, setHash: "", setTimer: null,
};
let tgbOn = false;

async function tgbApi(method: string, body: Record<string, unknown>): Promise<any> {
  const r = await fetch(`https://api.telegram.org/bot${tgb.token}/${method}`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  const j = (await r.json().catch(() => null)) as any;
  if (!r.ok || !j?.ok) throw new Error(`${method}: ${j?.description ?? r.status}`);
  return j.result;
}
async function tgbSendDoc(filename: string, buf: Buffer, caption: string): Promise<TgbRef> {
  const form = new FormData();
  form.append("chat_id", tgb.chat);
  form.append("disable_notification", "true");
  form.append("caption", caption.slice(0, 900));
  form.append("document", new Blob([new Uint8Array(buf)], { type: "application/octet-stream" }), filename);
  const r = await fetch(`https://api.telegram.org/bot${tgb.token}/sendDocument`, { method: "POST", body: form });
  const j = (await r.json().catch(() => null)) as any;
  if (!r.ok || !j?.ok) throw new Error(`sendDocument: ${j?.description ?? r.status}`);
  return { i: j.result.document.file_id as string, m: j.result.message_id as number };
}
async function tgbDelete(ids: (number | null | undefined)[]): Promise<void> {
  for (const m of ids) {
    if (m === null || m === undefined) continue;
    try { await tgbApi("deleteMessage", { chat_id: tgb.chat, message_id: m }); } catch { /* старше 48 ч или уже удалено — не страшно */ }
  }
}
async function tgbDownload(fileId: string): Promise<Buffer> {
  const f = await tgbApi("getFile", { file_id: fileId });
  const r = await fetch(`https://api.telegram.org/file/bot${tgb.token}/${f.file_path}`);
  if (!r.ok) throw new Error(`скачивание: ${r.status}`);
  return Buffer.from(await r.arrayBuffer());
}
function tgbRun(cmd: string, args: string[], stdin?: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const p = spawn(cmd, args, { stdio: ["pipe", "ignore", "pipe"] });
    let err = "";
    p.stderr.on("data", (d) => { err += String(d); });
    p.on("error", reject);
    p.on("close", (code) => (code === 0 ? resolve() : reject(new Error(`${cmd} код ${code}: ${err.slice(0, 200)}`))));
    p.stdin.on("error", () => { /* закрылся раньше */ });
    p.stdin.end(stdin ?? "");
  });
}
/** Какие файлы класть в архив: корневые lab-*.json и research-lab-state.json (кроме настроек торговли — они отдельно) + файлы папок данных за последние N дней. */
function tgbFiles(keepDays: number): { names: string[]; bytes: number } {
  const names: string[] = [];
  let bytes = 0;
  const cutoffDay = new Date(Date.now() - keepDays * 86400_000).toISOString().slice(0, 10);
  const cutoffMs = Date.now() - keepDays * 86400_000;
  const add = (rel: string, st: fs.Stats) => { names.push(rel); bytes += st.size; };
  for (const n of fs.readdirSync(STORAGE_DIR)) {
    if (n === "lab-live-config.json") continue;
    if (!(/^lab-.*\.json$/.test(n) || n === "research-lab-state.json")) continue;
    try { const st = fs.statSync(path.join(STORAGE_DIR, n)); if (st.isFile()) add(n, st); } catch { /* пропуск */ }
  }
  const walk = (rel: string) => {
    let list: string[] = [];
    try { list = fs.readdirSync(path.join(STORAGE_DIR, rel)); } catch { return; }
    for (const n of list) {
      const r2 = `${rel}/${n}`;
      let st: fs.Stats;
      try { st = fs.statSync(path.join(STORAGE_DIR, r2)); } catch { continue; }
      if (st.isDirectory()) { walk(r2); continue; }
      const dm = n.match(/(\d{4}-\d{2}-\d{2})/);
      if (dm ? dm[1] >= cutoffDay : st.mtimeMs >= cutoffMs) add(r2, st);
    }
  };
  for (const d of TGB_DIRS) walk(d);
  return { names, bytes };
}
function tgbManText(m: TgbManifest): string { return TGB_TAG + JSON.stringify(m); }
async function tgbPostManifest(m: TgbManifest): Promise<number> {
  const sent = await tgbApi("sendMessage", { chat_id: tgb.chat, text: tgbManText(m), disable_notification: true });
  try { await tgbApi("pinChatMessage", { chat_id: tgb.chat, message_id: sent.message_id, disable_notification: true }); }
  catch (e) { console.log("[tg-backup] не удалось закрепить сообщение:", (e as Error).message); throw new Error("не удалось закрепить сообщение со списком (в группе боту нужно право закреплять сообщения)"); }
  return sent.message_id as number;
}

/** Полная копия: настройки + данные за N дней. Возвращает текст для чата. */
async function tgbBackup(why: string): Promise<string> {
  if (!tgbOn) return "Копия в Telegram выключена (есть постоянный диск или LAB_TG_BACKUP=off).";
  if (tgb.busy) return "Копия уже делается — подожди пару минут.";
  tgb.busy = true;
  const tmp = path.join(os.tmpdir(), `lab-bk-${Date.now()}.tar.gz`);
  const uploaded: TgbRef[] = [];
  try {
    let keep = TGB_KEEP_DAYS, size = 0, names: string[] = [];
    for (;;) {
      const f = tgbFiles(keep);
      names = f.names;
      if (names.length === 0) return "Копия: пока нет файлов данных.";
      await tgbRun("tar", ["czf", tmp, "-C", STORAGE_DIR, "-T", "-"], names.join("\n"));
      size = fs.statSync(tmp).size;
      if (size <= TGB_PART * TGB_MAX_PARTS || keep <= 1) break;
      keep--; // слишком много — берём на день меньше
    }
    if (size > TGB_PART * TGB_MAX_PARTS) return `Копия не отправлена: даже за 1 день архив ${(size / 1048576).toFixed(0)} МБ — больше лимита.`;
    const fd = fs.openSync(tmp, "r");
    const parts: TgbRef[] = [];
    try {
      const n = Math.ceil(size / TGB_PART);
      for (let k = 0; k < n; k++) {
        const len = Math.min(TGB_PART, size - k * TGB_PART);
        const buf = Buffer.alloc(len);
        fs.readSync(fd, buf, 0, len, k * TGB_PART);
        const ref = await tgbSendDoc(`lab-backup-${k + 1}-of-${n}.bin`, buf, `LAB копия: часть ${k + 1}/${n} (${why})`);
        uploaded.push(ref);
        parts.push(ref);
      }
    } finally { fs.closeSync(fd); }
    let settings: TgbRef | null = null;
    try {
      const sb = fs.readFileSync(LIVE_CFG_FILE);
      settings = await tgbSendDoc("lab-live-config.json", sb, "LAB копия: настройки торговли");
      uploaded.push(settings);
      tgb.setHash = sb.toString("utf8");
    } catch { /* файла настроек ещё нет */ }
    const man: TgbManifest = { t: Date.now(), s: settings, p: parts, d: keep, b: size };
    const manMsg = await tgbPostManifest(man);
    const old = tgb.man, oldMan = tgb.manMsg;
    tgb.man = man;
    tgb.manMsg = manMsg;
    if (old) await tgbDelete([oldMan, old.s?.m, ...old.p.map((x) => x.m)]);
    return `💾 Копия в Telegram готова (${why}): ${names.length} файлов за ${keep} дн., ${(size / 1048576).toFixed(1)} МБ, частей: ${parts.length}${settings ? ", настройки отдельно" : ""}. Закреплено сообщение со списком.`;
  } catch (e) {
    await tgbDelete(uploaded.map((x) => x.m));
    console.log("[tg-backup] ошибка:", (e as Error).message);
    return `⚠️ Копия в Telegram не удалась: ${(e as Error).message}`;
  } finally {
    tgb.busy = false;
    try { fs.unlinkSync(tmp); } catch { /* нет файла */ }
  }
}

/** Только настройки (после «лаб объем», «лаб тс», «лаб лимит» и т.п.): маленький файл + новое закреплённое сообщение. */
function tgbSettingsSoon(): void {
  if (!tgbOn) return;
  if (tgb.setTimer) clearTimeout(tgb.setTimer);
  tgb.setTimer = setTimeout(() => { tgb.setTimer = null; void tgbSettingsNow(); }, 30_000);
}
async function tgbSettingsNow(): Promise<void> {
  if (!tgbOn) return;
  if (tgb.busy) { tgb.setTimer = setTimeout(() => { tgb.setTimer = null; void tgbSettingsNow(); }, 20_000); return; }
  tgb.busy = true;
  let ref: TgbRef | null = null;
  try {
    const sb = fs.readFileSync(LIVE_CFG_FILE);
    if (sb.toString("utf8") === tgb.setHash) return;
    ref = await tgbSendDoc("lab-live-config.json", sb, "LAB копия: настройки торговли");
    const man: TgbManifest = { t: Date.now(), s: ref, p: tgb.man?.p ?? [], d: tgb.man?.d ?? 0, b: tgb.man?.b ?? 0 };
    const manMsg = await tgbPostManifest(man);
    const old = tgb.man, oldMan = tgb.manMsg;
    tgb.man = man;
    tgb.manMsg = manMsg;
    tgb.setHash = sb.toString("utf8");
    await tgbDelete([oldMan, old?.s?.m]);
  } catch (e) {
    if (ref) await tgbDelete([ref.m]);
    console.log("[tg-backup] настройки не отправились:", (e as Error).message);
  } finally { tgb.busy = false; }
}

/** При старте на чистом диске: достать закреплённое сообщение, скачать куски и развернуть. Возвращает текст для чата или null. */
async function tgbRestore(): Promise<string | null> {
  if (!tgbOn) return null;
  if (fs.existsSync(STATE_FILE) || fs.existsSync(LIVE_CFG_FILE) || fs.existsSync(path.join(STORAGE_DIR, "lab-data"))) return null; // диск не пустой — ничего не трогаем
  try {
    const chat = await tgbApi("getChat", { chat_id: tgb.chat });
    const pm = chat?.pinned_message;
    const text: string = pm?.text ?? "";
    if (!text.startsWith(TGB_TAG)) return null;
    const man = JSON.parse(text.slice(TGB_TAG.length)) as TgbManifest;
    const tmp = path.join(os.tmpdir(), `lab-restore-${Date.now()}.tar.gz`);
    let n = 0;
    if (man.p.length > 0) {
      const bufs: Buffer[] = [];
      for (const ref of man.p) bufs.push(await tgbDownload(ref.i));
      fs.writeFileSync(tmp, Buffer.concat(bufs));
      fs.mkdirSync(STORAGE_DIR, { recursive: true });
      await tgbRun("tar", ["xzf", tmp, "-C", STORAGE_DIR]);
      try { fs.unlinkSync(tmp); } catch { /* нет файла */ }
      n = man.p.length;
    }
    let setTxt = "настроек в копии не было";
    if (man.s) {
      const sb = await tgbDownload(man.s.i);
      fs.writeFileSync(LIVE_CFG_FILE, sb);
      tgb.setHash = sb.toString("utf8");
      setTxt = "настройки торговли восстановлены";
    }
    tgb.man = man;
    tgb.manMsg = pm.message_id as number;
    return `♻️ Диск был пустой — восстановил данные из копии в Telegram (от ${new Date(man.t).toISOString().replace("T", " ").slice(0, 16)} UTC): частей ${n}, ${(man.b / 1048576).toFixed(1)} МБ, ${setTxt}.`;
  } catch (e) {
    console.log("[tg-backup] восстановление не удалось:", (e as Error).message);
    return `⚠️ Диск пустой, но восстановить из Telegram не вышло: ${(e as Error).message}`;
  }
}
function tgbInit(token: string | undefined, chat: string | undefined): void {
  if (!TGB_ENABLED || !token || !chat) return;
  tgb.token = token;
  tgb.chat = chat;
  tgbOn = true;
}
function tgbStart(): void {
  if (!tgbOn) return;
  setTimeout(() => void tgbBackup("по расписанию").then((t) => console.log("[tg-backup]", t)), 10 * 60 * 1000);
  setInterval(() => void tgbBackup("по расписанию").then((t) => console.log("[tg-backup]", t)), TGB_HOURS * 3600 * 1000);
}

// ───── v16: журнал сделок («лаб сделки») ─────
// По каждому сигналу ТС с торговлей хранится «карточка»: что было с сигналом (купили / цена убежала / ошибка биржи / цена не дошла / пропущено) и чем кончился рынок.
// Нужен, чтобы сверять бота со страницей по строкам. Ничего не меняет в торговле: только записи. Файл lab-trades-log.json (попадает и в копию Telegram).
type TjSt = "skip" | "watch" | "nohit" | "miss" | "paper" | "virtual" | "bought" | "failed" | "limit";
interface Tj {
  k: string; cfg: string; coin: string; side: "Up" | "Down"; slug: string; close: number;
  sig: number; sigPx: number; want?: number; cap?: number;
  st: TjSt; why?: string;
  at?: number; px?: number; sh?: number; usd?: number; // покупка (реальная или бумажная)
  miss?: number; missAt?: number; missN?: number; // цена дошла, но ask был выше потолка (первый случай)
  won?: boolean; pnl?: number; wp?: number; stop?: boolean; // итог рынка; pnl — по купленному, wp — «что было бы» по непокупленному
  // v17: «теневая» проверка раз в секунду (вход после 2 замеров подряд), без покупки — только для сравнения
  s1on?: boolean; s1?: number; s1At?: number; s1p?: number; s1Miss?: number; nA1?: number; // s1 — по какой цене она бы купила, s1p — итог
  s2on?: boolean; s2?: number; s2At?: number; s2p?: number; s2Miss?: number; // v18: «если бы вход был по LAB_SHADOW_PX (0.87)»: s2 — цена, по которой купила бы, s2p — итог
  nA?: number; mb?: number; lb?: number; la?: number; lt?: number; // nA — сколько замеров «цена дошла, но нет цены продажи»; mb — макс. bid; lb/la/lt — последний замер
  created: number;
}
const SHADOW_PX = Number(process.env.LAB_SHADOW_PX ?? "0.87"); // v18: теневой уровень входа для сравнения (0 — выключить)
const TJ_FILE = path.resolve(path.dirname(LIVE_CFG_FILE), "lab-trades-log.json");
const TJ_KEEP_MS = 72 * 3600_000;
const TJ_MAX = 4000;
const TJ_TZ = Number(process.env.LAB_TZ_HOURS ?? "7"); // часовой пояс для времени в «лаб сделки» (по умолчанию Бангкок, UTC+7)

function tjTime(ms: number): string {
  return new Date(ms + TJ_TZ * 3600_000).toISOString().slice(11, 19);
}
const tjMoney = (x: number): string => `${x >= 0 ? "+" : "−"}$${Math.abs(x).toFixed(2)}`;

/** Что стало с рынком для карточек этого рынка: исход, «что было бы», окончание слежения. */
function tjSettleCards(cards: Iterable<Tj>, slug: string, winner: "Up" | "Down", cfgs: TradeCfg[]): void {
  for (const c of cards) {
    if (c.slug !== slug) continue;
    c.won = c.side === winner;
    if (c.st === "watch") c.st = c.miss !== undefined ? "miss" : "nohit";
    const cfg = cfgs.find((x) => x.name === c.cfg);
    if (cfg && c.s2 !== undefined) { const sh2 = sharesOf(cfg, c.s2); c.s2p = c.won ? sh2 * (1 - c.s2) : -sh2 * c.s2; }
    if (cfg && c.s1 !== undefined) { const sh1 = sharesOf(cfg, c.s1); c.s1p = c.won ? sh1 * (1 - c.s1) : -sh1 * c.s1; }
    if (c.st === "bought" || c.st === "paper") continue;
    if (!cfg) continue;
    const px = c.want ?? c.sigPx;
    const sh = sharesOf(cfg, px);
    c.wp = c.won ? sh * (1 - px) : -sh * px;
  }
}

/** Текст «лаб сделки»: строки по времени (как на странице: время покупки / момент, когда цена дошла) + сводка. */
function tjFormat(cards: Tj[], opts: { cfg: string | null; hours: number; all: boolean; now: number }): string[] {
  const from = opts.now - opts.hours * 3600_000;
  let list = cards.filter((c) => c.sig >= from && (opts.cfg === null || c.cfg.toLowerCase() === opts.cfg));
  const hidden = list.filter((c) => c.st === "skip").length;
  if (!opts.all) list = list.filter((c) => c.st !== "skip");
  const when = (c: Tj): number => c.at ?? c.missAt ?? c.sig;
  list.sort((a, b) => when(a) - when(b));
  const per = new Map<string, Tj[]>();
  for (const c of list) { if (!per.has(c.cfg)) per.set(c.cfg, []); per.get(c.cfg)!.push(c); }
  const head: string[] = [];
  head.push(`📒 LAB сделки за ${opts.hours >= 48 ? `${Math.round(opts.hours / 24)} дн.` : `${opts.hours} ч`}${opts.cfg ? ` · ТС ${opts.cfg}` : ""} (время: UTC${TJ_TZ >= 0 ? "+" : ""}${TJ_TZ})`);
  if (list.length === 0) { head.push(hidden > 0 ? `Сделок нет (скрыто ${hidden} сигналов без покупки: фильтр/выкл/лимит — «лаб сделки все»).` : "Сделок за этот период нет."); return [head.join("\n")]; }
  for (const [name, cs] of per) {
    const bought = cs.filter((c) => c.st === "bought" || c.st === "paper");
    const settled = bought.filter((c) => c.pnl !== undefined);
    const w = settled.filter((c) => (c.pnl as number) >= 0).length;
    const pnl = settled.reduce((a, c) => a + (c.pnl as number), 0);
    const pend = bought.length - settled.length;
    const px = bought.length ? bought.reduce((a, c) => a + (c.px ?? 0), 0) / bought.length : null;
    const missed = cs.filter((c) => c.st === "miss" || c.st === "failed" || c.st === "limit");
    const mw = missed.filter((c) => c.won === true).reduce((a, c) => a + (c.wp ?? 0), 0); // упущено
    const ml = missed.filter((c) => c.won === false).reduce((a, c) => a + (c.wp ?? 0), 0); // избежали (отрицательное)
    const nohit = cs.filter((c) => c.st === "nohit").length;
    const other = cs.filter((c) => c.st === "virtual" || c.st === "skip").length;
    head.push(
      `\n▸ ${name}: сигналов ${cs.length}\n` +
        `  купили ${bought.length}${px !== null ? ` (ср. цена ${px.toFixed(3)})` : ""}: ${w} побед / ${settled.length - w} проигр., итог ${tjMoney(pnl)}${pend ? `, ещё идёт ${pend}` : ""}\n` +
        `  не купили из-за цены/ошибки: ${missed.length} (упущено ${tjMoney(mw)}, избежали ${tjMoney(ml)}); цена не дошла: ${nohit}${other ? `; прочее: ${other}` : ""}`,
    );
    const nNoAsk = cs.filter((c) => (c.st === "nohit" || c.st === "watch") && (c.nA ?? 0) > 0).length;
    if (nNoAsk > 0) head.push(`  ⚠️ из «цена не дошла» у ${nNoAsk} цена на деле дошла, но у рынка не было цены продажи (ask)`);
    if (SHADOW_PX > 0 && cs.some((c) => c.s2on)) {
      const base = cs.filter((c) => c.s2on); // сигналы, за которыми теневой уровень реально следил
      const isB = (c: Tj): boolean => c.st === "bought" || c.st === "paper";
      const bBought = base.filter((c) => isB(c) && c.pnl !== undefined);
      const bSum = bBought.reduce((a, c) => a + (c.pnl as number), 0), bL = bBought.filter((c) => (c.pnl as number) < 0).length;
      const s2 = base.filter((c) => c.s2 !== undefined && c.s2p !== undefined);
      const s2Sum = s2.reduce((a, c) => a + (c.s2p as number), 0), s2L = s2.filter((c) => (c.s2p as number) < 0).length;
      const skipped = bBought.filter((c) => c.s2 === undefined); // бот купил, а на 0.87 не вошла бы
      const skL = skipped.filter((c) => (c.pnl as number) < 0), skW = skipped.length - skL.length;
      const skSum = skipped.reduce((a, c) => a + (c.pnl as number), 0);
      const extra = s2.filter((c) => !isB(c)); // бот не купил, а на 0.87 купила бы
      const exSum2 = extra.reduce((a, c) => a + (c.s2p as number), 0);
      const both2 = s2.filter((c) => isB(c) && c.pnl !== undefined);
      const bothBot = both2.reduce((a, c) => a + (c.pnl as number), 0), bothSh = both2.reduce((a, c) => a + (c.s2p as number), 0);
      const avgB = bBought.length ? bBought.reduce((a, c) => a + (c.px ?? 0), 0) / bBought.length : 0;
      const avgS = s2.length ? s2.reduce((a, c) => a + (c.s2 as number), 0) / s2.length : 0;
      head.push(
        `  🔬 Если бы вход был по ${SHADOW_PX} (та же проверка раз в 5с, без покупки):\n` +
          `     бот сейчас: ${bBought.length} сделок, проигр. ${bL}, итог ${tjMoney(bSum)}, ср. цена ${avgB.toFixed(3)}\n` +
          `     вход по ${SHADOW_PX}: ${s2.length} сделок, проигр. ${s2L}, итог ${tjMoney(s2Sum)}, ср. цена ${avgS.toFixed(3)}\n` +
          `     не вошла бы туда, где бот купил: ${skipped.length} (побед ${skW}, проигр. ${skL.length}; бот на них ${tjMoney(skSum)})\n` +
          `     вошла бы там, где бот не купил: ${extra.length} (${tjMoney(exSum2)})\n` +
          `     на общих ${both2.length} сделках: бот ${tjMoney(bothBot)}, вход по ${SHADOW_PX} ${tjMoney(bothSh)}`,
      );
    }
    if (cs.some((c) => c.s1on)) {
      const shS = cs.filter((c) => c.s1 !== undefined && c.s1p !== undefined);
      const isBot = (c: Tj): boolean => c.st === "bought" || c.st === "paper";
      const extra = shS.filter((c) => !isBot(c)); // бот не купил, а проверка раз в секунду купила бы
      const exW = extra.filter((c) => (c.s1p as number) >= 0);
      const exSum = extra.reduce((a, c) => a + (c.s1p as number), 0);
      const lostBy = cs.filter((c) => isBot(c) && c.pnl !== undefined && c.s1on && c.s1 === undefined); // бот купил, проверка раз в секунду не купила бы
      const lbSum = lostBy.reduce((a, c) => a + (c.pnl as number), 0);
      const both = shS.filter((c) => isBot(c) && c.pnl !== undefined);
      const diff = both.reduce((a, c) => a + ((c.s1p as number) - (c.pnl as number)), 0);
      head.push(
        `  🔬 Проверка раз в секунду (2 замера подряд, без покупки): купила бы ${shS.length}, итог ${tjMoney(shS.reduce((a, c) => a + (c.s1p as number), 0))}
` +
          `     вернула бы пропущенное ботом: ${extra.length} (${exW.length} побед / ${extra.length - exW.length} проигр., ${tjMoney(exSum)})
` +
          `     не взяла бы то, что бот купил: ${lostBy.length} (бот на них ${tjMoney(lbSum)})
` +
          `     там, где купили обе: ${both.length}, разница цены входа в её пользу/против ${tjMoney(diff)}`,
      );
    }
  }
  if (hidden > 0 && !opts.all) head.push(`\n(скрыто ${hidden} сигналов без покупки: фильтр/выкл/лимит — «лаб сделки все»)`);
  const lines: string[] = [];
  for (const c of list) {
    const t = tjTime(when(c));
    const base = `${t} ${c.coin} ${c.side}${opts.cfg ? "" : ` [${c.cfg}]`}`;
    const outcome = (c.won === undefined ? "⏳ рынок идёт" : c.won ? "рынок выиграл" : "рынок проиграл");
    const wasTxt = c.wp !== undefined ? (c.won ? `упущено ${tjMoney(c.wp)}` : `избежали ${tjMoney(c.wp)}`) : outcome;
    let s: string;
    if (c.st === "bought" || c.st === "paper") {
      const res = c.pnl === undefined ? "⏳ идёт" : `${c.pnl >= 0 ? "✅" : "❌"} ${tjMoney(c.pnl)}${c.stop ? " (стоп)" : ""}`;
      const slip = c.want !== undefined && c.px !== undefined ? ` (на странице ${c.want.toFixed(2)})` : "";
      s = `${base} · ${c.st === "paper" ? "📝" : "💰"} купили по ${(c.px ?? 0).toFixed(3)}${slip}, ${(c.sh ?? 0).toFixed(2)} акц. → ${res} · сигнал ${tjTime(c.sig)} @${c.sigPx.toFixed(2)}`;
    } else if (c.st === "miss") {
      s = `${base} · 🚫 цена дошла до ${(c.want ?? 0).toFixed(2)}, но ask ${(c.miss ?? 0).toFixed(2)} выше потолка ${(c.cap ?? 0).toFixed(2)} → ${wasTxt}`;
    } else if (c.st === "failed") {
      s = `${base} · 🚫 не вышло: ${c.why ?? "ошибка"} → ${wasTxt}`;
    } else if (c.st === "limit") {
      s = `${base} · ⛔ ${c.why ?? "лимит"} → ${wasTxt}`;
    } else if (c.st === "nohit") {
      if ((c.nA ?? 0) > 0) s = `${base} · ⚠️ цена дошла до ${(c.want ?? 0).toFixed(2)}, но у рынка не было цены продажи (ask), замеров ${c.nA} · ${outcome}`;
      else {
        const lastTxt = c.lb !== undefined && c.lt !== undefined ? `, последний замер bid ${c.lb.toFixed(2)}${c.la !== undefined ? `/ask ${c.la.toFixed(2)}` : "/ask нет"} за ${Math.max(0, Math.round((c.close - c.lt) / 1000))}с до конца` : "";
        s = `${base} · ⚪ цена не дошла до ${(c.want ?? 0).toFixed(2)} (сигнал @${c.sigPx.toFixed(2)}${c.mb !== undefined ? `, макс. bid ${c.mb.toFixed(2)}` : ""}${lastTxt}) · ${outcome}`;
      }
    } else if (c.st === "watch") {
      s = `${base} · 👀 ждём цену ${(c.want ?? 0).toFixed(2)} (сигнал @${c.sigPx.toFixed(2)})`;
    } else {
      s = `${base} · ➖ ${c.why ?? "не торговали"} → ${wasTxt}`;
    }
    if (c.s1 !== undefined) s += ` · 🔬 раз в сек: купила бы по ${c.s1.toFixed(3)}${c.s1At !== undefined ? ` в ${tjTime(c.s1At)}` : ""}${c.s1p !== undefined ? ` → ${tjMoney(c.s1p)}` : ""}`;
    if (c.s2on) s += c.s2 !== undefined ? ` · 🔬${SHADOW_PX}: купила бы по ${c.s2.toFixed(3)} в ${tjTime(c.s2At as number)}${c.s2p !== undefined ? ` → ${tjMoney(c.s2p)}` : ""}` : ` · 🔬${SHADOW_PX}: не вошла${c.s2Miss !== undefined ? ` (ask ${c.s2Miss.toFixed(2)} выше потолка)` : ""}`;
    lines.push(s);
  }
  // режем на сообщения до ~3500 символов
  const out: string[] = [];
  let cur = head.join("\n");
  for (const ln of lines) {
    if (cur.length + ln.length + 1 > 3500) { out.push(cur); cur = ""; }
    cur += (cur ? "\n" : "") + ln;
  }
  if (cur) out.push(cur);
  return out;
}

const PAGE_COIN_WORDS: Record<string, string> = {
  btc: "Bitcoin", биткоин: "Bitcoin", eth: "Ethereum", эфир: "Ethereum", sol: "Solana", солана: "Solana",
  xrp: "XRP", doge: "Dogecoin", дож: "Dogecoin",
};

/** v4.6: разбор аргументов страницы (общий для Telegram-команды и ссылки). */
function parsePageArgs(args: string[]): { days: number | null; label: string; wm?: number; coin?: string } {
  let days: number | null = null;
  let label = "всё время";
  let wm: number | undefined;
  let coin: string | undefined;
  for (const a of args) {
    let m: RegExpMatchArray | null;
    if (a === "все" || a === "всё" || a === "all") {
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
  return { days, label, wm, coin };
}

// v4.6: ссылка на страницу без лимита 50 МБ. Включается переменной LAB_PAGE_TOKEN (любой длинный секретный текст)
// + публичный домен сервиса в Railway (Settings → Networking → Generate Domain). Адрес: https://<домен>/page/<токен>?p=all
const PAGE_TOKEN = process.env.LAB_PAGE_TOKEN ?? "";
const pageCache = new Map<string, { at: number; html: string }>();
const pageJobs = new Map<string, { at: number; done: boolean; err: string | null }>();
function pageLinkBase(): string | null {
  const dom = process.env.RAILWAY_PUBLIC_DOMAIN ?? process.env.LAB_PAGE_DOMAIN;
  if (!PAGE_TOKEN || !dom) return null;
  return `https://${dom}/page/${PAGE_TOKEN}`;
}
function pageLinkWhy(): string {
  const miss: string[] = [];
  if (!PAGE_TOKEN) miss.push("нет переменной LAB_PAGE_TOKEN (Variables)");
  if (!(process.env.RAILWAY_PUBLIC_DOMAIN ?? process.env.LAB_PAGE_DOMAIN)) miss.push("нет домена (Settings → Networking → Generate Domain; либо вручную переменная LAB_PAGE_DOMAIN = адрес без https://)");
  return miss.join("; ");
}
function startPageServer(): void {
  if (!PAGE_TOKEN) {
    console.log("[page] ссылка выключена: нет LAB_PAGE_TOKEN");
    return;
  }
  const port = Number(process.env.PORT ?? "8080") || 8080; // если Railway не задал PORT — слушаем 8080 (в Networking укажи этот порт)
  const srv = http.createServer((req, res) => {
    void (async () => {
      try {
        const url = new URL(req.url ?? "/", "http://x");
        const parts = url.pathname.split("/").filter(Boolean);
        if (parts[0] === "ping") {
          res.end("ok");
          return;
        }
        if (parts[0] !== "page" || parts[1] !== PAGE_TOKEN) {
          res.statusCode = 404;
          res.end("not found");
          return;
        }
        // v13: скачать ВСЁ, что лежит на диске (volume): /page/<токен>/backup — архив .tar.gz; ?part=small — только настройки и счётчики (маленькие .json в корне)
        if (parts[2] === "backup") {
          const dir = path.resolve(STORAGE_DIR);
          const small = url.searchParams.get("part") === "small";
          const day = new Date().toISOString().slice(0, 10);
          let files: string[] = ["."];
          if (small) files = fs.readdirSync(dir).filter((f) => f.endsWith(".json") && fs.statSync(path.join(dir, f)).isFile());
          if (files.length === 0) {
            res.statusCode = 404;
            res.end("нет файлов");
            return;
          }
          res.setHeader("Content-Type", "application/gzip");
          res.setHeader("Content-Disposition", `attachment; filename="lab-backup${small ? "-small" : ""}-${day}.tar.gz"`);
          const tar = spawn("tar", ["czf", "-", "-C", dir, ...files]);
          tar.stdout.pipe(res);
          tar.stderr.on("data", (d) => console.log("[backup] tar:", String(d).slice(0, 200)));
          tar.on("error", (e) => {
            console.log("[backup] не запустился tar:", e.message);
            if (!res.headersSent) res.statusCode = 500;
            res.end();
          });
          req.on("close", () => tar.kill());
          return;
        }
        const p = (url.searchParams.get("p") ?? "all").toLowerCase();
        const args = p.split(",").filter(Boolean);
        const co = url.searchParams.get("coin");
        if (co) args.push(co.toLowerCase());
        const wmq = url.searchParams.get("wm");
        if (wmq) args.push(wmq);
        const key = args.join(" ");
        const hit = pageCache.get(key);
        let html: string | null = null;
        if (hit && Date.now() - hit.at < 5 * 60_000 && url.searchParams.get("fresh") !== "1") html = hit.html;
        else {
          // сборка идёт в фоне: сразу отвечаем «собираю», страница сама обновляется, пока не будет готово
          let job = pageJobs.get(key);
          if (!job || (job.done && url.searchParams.get("fresh") === "1" && Date.now() - job.at > 20_000)) {
            const a = parsePageArgs(args);
            const sinceMs = a.days === null ? 0 : Date.now() - a.days * DAY_MS;
            const j: { at: number; done: boolean; err: string | null } = { at: Date.now(), done: false, err: null };
            pageJobs.set(key, j);
            job = j;
            void (async () => {
              try {
                const r = await buildPage(LAB_DATA_DIR, { sinceMs, periodLabel: a.label, coin: a.coin, wm: a.wm }, 1_500_000_000);
                pageCache.clear();
                pageCache.set(key, { at: Date.now(), html: r.html });
              } catch (e) {
                j.err = (e as Error).message;
              } finally {
                j.done = true;
              }
            })();
          }
          const cur = pageCache.get(key);
          if (cur) html = cur.html;
          else {
            const q = new URL(req.url ?? "/", "http://x");
            q.searchParams.delete("fresh");
            res.statusCode = 200;
            res.setHeader("Content-Type", "text/html; charset=utf-8");
            res.setHeader("Cache-Control", "no-store");
            res.end(`<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">${job.err ? "" : `<meta http-equiv="refresh" content="6;url=${q.pathname}${q.search}">`}<body style="font-family:sans-serif;background:#111;color:#eee;padding:30px">${job.err ? `<h3>Ошибка сборки</h3><p>${job.err.replace(/</g, "&lt;")}</p>` : `<h3>⏳ Собираю страницу…</h3><p>Прошло ${Math.round((Date.now() - job.at) / 1000)} с. Страница обновится сама, обычно до 1–2 минут.</p>`}</body>`);
            return;
          }
        }
        res.setHeader("Content-Type", "text/html; charset=utf-8");
        res.setHeader("Cache-Control", "no-store");
        res.setHeader("X-Robots-Tag", "noindex");
        res.end(html ?? "");
      } catch (err) {
        res.statusCode = 500;
        res.end("error: " + (err as Error).message);
      }
    })();
  });
  srv.listen(port, () => console.log(`[page] ссылка на страницу включена (порт ${port}, домен ${process.env.RAILWAY_PUBLIC_DOMAIN ?? process.env.LAB_PAGE_DOMAIN ?? "не задан"})`));
  srv.on("error", (e) => console.log("[page] сервер ссылки не запустился:", (e as Error).message));
}

/** «лаб страница [24ч|3д|7д|30д|все] [5|15] [btc|eth|sol|xrp|doge]» */
async function handlePageCommand(
  botToken: string,
  chatId: string,
  telegram: ReturnType<typeof createTelegramNotifier>,
  text: string,
): Promise<void> {
  const args = text.replace(/^(лаб страница|\/lab_page)/, "").trim().split(" ").filter(Boolean);
  const { days, label, wm, coin } = parsePageArgs(args);
  // v4.9: если настроена ссылка — шлём её вместо файла (без лимита 50 МБ и без выборки). Файл по-прежнему можно: «лаб страница файл»
  const linkBase = pageLinkBase();
  if (!linkBase && !args.includes("файл")) await telegram?.send(`ℹ️ Ссылка не настроена: ${pageLinkWhy()}. Пока шлю файлом.`);
  if (linkBase && !args.includes("файл")) {
    const per = args.filter((x) => x !== "файл").join(",") || "all";
    await telegram?.send(`🔗 Страница-разбор, ВСЕ данные без лимита:\n${linkBase}?p=${encodeURIComponent(per)}&fresh=1\n(открывается через ~минуту, пока собирается; ссылка секретная). Файлом: «лаб страница файл».`);
    return;
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
    const link = pageLinkBase();
    let ok = Buffer.byteLength(html) <= 49_000_000 && (await sendDocumentToTelegram(botToken, chatId, name, html, cap));
    // v4.6: не влезло — пересобираем с меньшим числом строк (меньший лимит), пока Telegram не примет
    for (const lim of [38, 28, 20]) {
      if (ok) break;
      await telegram?.send(`Файл не влез в 50 МБ, пробую сделать меньше (до ${lim} МБ, часть строк будет выборкой)…`);
      const html2 = await shrinkPage(data, lim * 1024 * 1024);
      const m2 = data.meta as { markets: number; trades: number; rowsEmbedded: number; sampled: boolean };
      ok = await sendDocumentToTelegram(botToken, chatId, name, html2, `LAB: страница-разбор (${label}), выборка: ${m2.rowsEmbedded} из ${m2.trades} сделок. Полная версия без лимита — по ссылке («лаб ссылка»).`);
    }
    if (!ok) await telegram?.send("Не удалось отправить файл. Попробуй короче период: «лаб страница 24ч» или по монете." + (link ? `\nПолная страница по ссылке: ${link}?p=all` : ""));
    else {
      console.log(`[page] готово за ${((Date.now() - t0) / 1000).toFixed(1)}с, ${(html.length / 1048576).toFixed(1)} МБ`);
      if (link && Buffer.byteLength(html) > 30_000_000) await telegram?.send(`Страница уже большая. Без лимита и без скачивания: ${link}?p=all`);
    }
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

        if (text === "лаб тест" || text.startsWith("лаб тест ")) {
          const arg = text.replace(/^лаб тест/, "").trim();
          if (arg === "сброс") {
            lab.testReset();
            await telegram?.send(escapeTelegramHtml("🧪 ЛАБ ТЕСТ: данные сброшены, считаем заново."));
          } else await telegram?.send(escapeTelegramHtml(lab.testText(arg || undefined)));
          continue;
        }

        if (text === "лаб лимитки" || text.startsWith("лаб лимитки ")) {
          const arg = text.replace(/^лаб лимитки/, "").trim();
          if (arg === "сброс") { lab.limReset(); await telegram?.send("🪤 Бумажные лимитки сброшены."); }
          else await telegram?.send(escapeTelegramHtml(lab.limText(arg || undefined)));
          continue;
        }

        if (text === "лаб бэкап" || text === "лаб бекап" || text === "лаб копия") {
          await telegram?.send("⏳ Делаю копию данных в Telegram… (может занять минуту-две)");
          await telegram?.send(escapeTelegramHtml(await tgbBackup("вручную")));
          continue;
        }

        if (text === "лаб сделки" || text.startsWith("лаб сделки ")) {
          const arg = text.replace(/^лаб сделки/, "").trim();
          for (const part of lab.tradesText(arg)) await telegram?.send(escapeTelegramHtml(part));
          continue;
        }

        if (text === "лаб минимум") {
          await telegram?.send(escapeTelegramHtml(await lab.minInfoText()));
          continue;
        }

        if (text === "лаб проба" || text.startsWith("лаб проба ")) {
          const m = text.match(/^лаб проба да(?:\s+([\d.,]+))?(?:\s+([a-zа-я]+))?$/);
          const cw = m && m[2] ? PAGE_COIN_WORDS[m[2]] : undefined; // v14: монета после «да» (btc / eth / sol / xrp / doge)
          if (m && m[2] && !cw) { await telegram?.send(escapeTelegramHtml(`Не знаю монету «${m[2]}». Пиши: btc, eth, sol, xrp или doge.`)); continue; }
          await telegram?.send(escapeTelegramHtml(await lab.probeSmallText(!!m, m && m[1] ? Number(m[1].replace(",", ".")) : null, cw ?? null)));
          continue;
        }

        if (text === "лаб после" || text.startsWith("лаб после ")) {
          const n = parseInt(text.replace(/^лаб после/, "").trim(), 10);
          await telegram?.send(escapeTelegramHtml(lab.postText(Number.isFinite(n) && n > 0 ? n : 3)));
          continue;
        }

        if (text === "лаб стоп") {
          await telegram?.send(escapeTelegramHtml(lab.paperStopText()));
          continue;
        }

        if (text === "лаб ссылка" || text.startsWith("лаб ссылка ")) {
          const base = pageLinkBase();
          const per = text.replace(/^лаб ссылка/, "").trim().split(" ").filter(Boolean).join(",") || "all";
          if (base) await telegram?.send(`🔗 Страница-разбор без лимита размера (собирается минуту, потом открывается сразу):\n${base}?p=${encodeURIComponent(per)}\nПериод: p=24ч, 3д, 7д, 30д, all; монета: &coin=btc; окно: &wm=5. Ссылка секретная — никому не давай.`);
          else await telegram?.send(`Ссылка не настроена: ${pageLinkWhy()}. После настройки сервис перезапустится сам; потом напиши «лаб ссылка».`);
          continue;
        }

        if (text === "лаб страница" || text.startsWith("лаб страница ") || text === "/lab_page" || text.startsWith("/lab_page ")) {
          console.log(`[telegram] команда: "${msg.text}" → страница`);
          void handlePageCommand(botToken, chatId, telegram, text);
          continue;
        }

        if (text === "лаб объем" || text.startsWith("лаб объем ") || text === "/lab_size" || text.startsWith("/lab_size ")) {
          const args = text.replace(/^(лаб объем|\/lab_size)/, "").trim().split(" ").filter(Boolean);
          const list = () => TRADE_CFGS.map((c) => `• ${c.on ? "" : "⏸ "}${c.name}: ${stakeTxt(c)}${c.shares ? ` (в долларах $${c.size}, не используется)` : ""} (минимум $${minStakeOf(c).toFixed(2)}, максимум $${MAX_STAKE}) — ${c.label}`).join("\n");
          if (args.length === 0) {
            await telegram?.send(escapeTelegramHtml(`Ставки LAB:\n${list()}\n\nИзменить: «лаб объем bot7b0 6», «лаб объем fair2 5», «лаб объем все 5». Действует на новые покупки.`));
          } else {
            const val = Number((args[1] ?? "").replace(",", "."));
            const targets = pickCfgs(args[0]);
            if (targets.length === 0) await telegram?.send(escapeTelegramHtml(`Не знаю стратегию «${args[0]}». Доступны: ${TRADE_CFGS.map((c) => c.name).join(", ")}, все.`));
            else if (!Number.isFinite(val) || val <= 0) await telegram?.send(escapeTelegramHtml(`Укажи сумму числом, например: лаб объем ${targets[0].name} 6`));
            else {
              const bad = targets.filter((c) => val < minStakeOf(c) || val > MAX_STAKE);
              if (bad.length > 0) await telegram?.send(escapeTelegramHtml(`Не меняю: ${bad.map((c) => `${c.name} допускает $${minStakeOf(c).toFixed(2)}–$${MAX_STAKE}`).join("; ")} (у биржи минимум $1 на покупку, у нас с запасом $1.05; максимум можно поднять переменной LAB_MAX_STAKE). Если у стратегии задано число акций, доллары не используются — меняй командой «лаб акций ...».`));
              else {
                for (const c of targets) c.size = val;
                saveLiveCfg();
                await telegram?.send(escapeTelegramHtml(`✅ Ставка изменена на $${val}: ${targets.map((c) => c.name).join(", ")}. Действует на новые покупки.\n${list()}`));
              }
            }
          }
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

        // v5.5: «вход после дна» — цены дна и покупки, вкл/выкл
        if (text === "лаб дно" || text.startsWith("лаб дно ") || text === "/lab_dip" || text.startsWith("/lab_dip ")) {
          const args = text.replace(/^(лаб дно|\/lab_dip)/, "").trim().split(/\s+/).filter(Boolean);
          const num = (x: string | undefined): number => Number((x ?? "").replace(",", "."));
          if (args.length === 0) await telegram?.send(escapeTelegramHtml(lab.dipStatusText()));
          else if (args[0] === "вкл" || args[0] === "выкл") {
            DIP.on = args[0] === "вкл";
            saveLiveCfg();
            await telegram?.send(escapeTelegramHtml(`${DIP.on ? "🟢 Вход после дна включён" : "⏸ Вход после дна выключен"}. Уже купленные позиции доигрываются.\n${lab.dipStatusText()}`));
          } else if (args[0] === "акций") {
            const n = num(args[1]);
            if (!Number.isFinite(n) || n < MIN_SHARES || n > MAX_SHARES) await telegram?.send(escapeTelegramHtml(`Акций от ${MIN_SHARES} до ${MAX_SHARES}. Пример: лаб дно акций 5`));
            else { DIP.shares = n; saveLiveCfg(); await telegram?.send(escapeTelegramHtml(`✅ Вход после дна: ${n} акций.`)); }
          } else if (args[0] === "мин") {
            const n = num(args[1]);
            if (!Number.isFinite(n) || n < 3 || n > 120) await telegram?.send(escapeTelegramHtml("Секунд от 3 до 120. Пример: лаб дно мин 15"));
            else { DIP.minLeft = n; saveLiveCfg(); await telegram?.send(escapeTelegramHtml(`✅ Не покупаю, если до закрытия осталось меньше ${n}с.`)); }
          } else if (args[0] === "спред") {
            const n = num(args[1]);
            if (!Number.isFinite(n) || n < 0 || n > 10) await telegram?.send(escapeTelegramHtml("Центов от 0 до 10. Пример: лаб дно спред 3"));
            else { DIP.spread = n; saveLiveCfg(); await telegram?.send(escapeTelegramHtml(`✅ Покупаю, если ask не выше ${DIP.buy + n}¢ (цена покупки ${DIP.buy}¢ + ${n}¢).`)); }
          } else {
            const lo = num(args[0]);
            const by = num(args[1]);
            if (!Number.isFinite(lo) || !Number.isFinite(by) || lo < 1 || by > 98 || lo >= by) await telegram?.send(escapeTelegramHtml("Два числа в центах: дно и покупка, дно меньше покупки (1-98). Пример: лаб дно 75 80"));
            else {
              DIP.low = lo;
              DIP.buy = by;
              saveLiveCfg();
              await telegram?.send(escapeTelegramHtml(`✅ Вход после дна: дно ≤${lo}¢, покупка, когда bid вернулся до ${by}¢ (ask не выше ${by + DIP.spread}¢). Новые цены действуют сразу, в том числе для уже отслеживаемых сигналов.`));
            }
          }
          continue;
        }

        // v4.0: управление стратегиями — вкл/выкл, ставка в акциях, дневные лимиты
        if (text === "лаб бумага" || text.startsWith("лаб бумага ")) {
          const args = text.replace(/^лаб бумага/, "").trim().split(" ").filter(Boolean);
          const act = args.find((a) => a === "вкл" || a === "выкл");
          const nm = args.find((a) => a !== "вкл" && a !== "выкл");
          if (!act || !nm) await telegram?.send(escapeTelegramHtml(`Бумажный режим: ТС считает виртуальные сделки, реальных денег не тратит (чтобы проверить новую ТС на настоящих днях).\nВключить: «лаб бумага t05 вкл» (или имя группы / «все»). Выключить: «лаб бумага t05 выкл».\nСейчас на бумаге: ${TRADE_CFGS.filter((c) => c.paper).map((c) => c.name).join(", ") || "никого"}.`));
          else {
            const targets = pickCfgs(nm);
            if (targets.length === 0) await telegram?.send(escapeTelegramHtml(`Не знаю стратегию «${nm}». Доступны: ${TRADE_CFGS.map((c) => c.name).join(", ")}, все.`));
            else {
              for (const c of targets) c.paper = act === "вкл";
              saveLiveCfg();
              await telegram?.send(escapeTelegramHtml(`✅ ${act === "вкл" ? "На бумагу (реальных денег нет)" : "Снова реальная торговля"}: ${targets.map((c) => c.name).join(", ")}.`));
            }
          }
          continue;
        }

        if (text === "лаб тс" || text.startsWith("лаб тс ") || text === "/lab_ts" || text.startsWith("/lab_ts ")) {
          const args = text.replace(/^(лаб тс|\/lab_ts)/, "").trim().split(" ").filter(Boolean);
          const act = args.find((a) => a === "вкл" || a === "выкл");
          const nm = args.find((a) => a !== "вкл" && a !== "выкл");
          const names = TRADE_CFGS.map((c) => c.name).join(", ");
          if (!act) {
            await telegram?.send(escapeTelegramHtml(`Стратегии LAB:\n${lab.dayLimitsText()}\n\nВыключить: «лаб тс выкл fair2». Включить: «лаб тс вкл cb4». Все сразу: «лаб тс выкл все». Выключенная не торгует, но статистика по ней пишется.`));
          } else {
            const targets = pickCfgs(nm ?? "");
            if (!nm || targets.length === 0) await telegram?.send(escapeTelegramHtml(`Назови стратегию: ${names} или «все». Пример: лаб тс ${act} ${TRADE_CFGS[0].name}`));
            else {
              for (const c of targets) c.on = act === "вкл";
              saveLiveCfg();
              await telegram?.send(escapeTelegramHtml(`${act === "вкл" ? "🟢 Включено" : "⏸ Выключено"}: ${targets.map((c) => c.name).join(", ")}. Уже открытые позиции доигрываются, новых покупок у выключенных нет.\n${lab.dayLimitsText()}`));
            }
          }
          continue;
        }

        if (text === "лаб акций" || text.startsWith("лаб акций ") || text === "/lab_shares" || text.startsWith("/lab_shares ")) {
          const args = text.replace(/^(лаб акций|\/lab_shares)/, "").trim().split(" ").filter(Boolean);
          const list = () => TRADE_CFGS.map((c) => `• ${c.name}: ${stakeTxt(c)}`).join("\n");
          if (args.length < 2) {
            await telegram?.send(escapeTelegramHtml(`Ставки:\n${list()}\n\nПокупать всегда N акций: «лаб акций cb4 5» (минимум ${MIN_SHARES}, максимум ${MAX_SHARES}). Вернуться к ставке в долларах: «лаб акций cb4 выкл».`));
          } else {
            const targets = pickCfgs(args[0]);
            const off = args[1] === "выкл";
            const val = Number(args[1].replace(",", "."));
            if (targets.length === 0) await telegram?.send(escapeTelegramHtml(`Не знаю стратегию «${args[0]}». Доступны: ${TRADE_CFGS.map((c) => c.name).join(", ")}, все.`));
            else if (!off && (!Number.isFinite(val) || val < MIN_SHARES || val > MAX_SHARES)) await telegram?.send(escapeTelegramHtml(`Число акций от ${MIN_SHARES} до ${MAX_SHARES}, например: лаб акций ${targets[0].name} 5`));
            else {
              for (const c of targets) c.shares = off ? undefined : val;
              saveLiveCfg();
              await telegram?.send(escapeTelegramHtml(`✅ ${targets.map((c) => c.name).join(", ")}: ${off ? "ставка снова в долларах" : `теперь всегда ${val} акций`}. Действует на новые покупки.\n${list()}`));
            }
          }
          continue;
        }

        if (text === "лаб лимит" || text.startsWith("лаб лимит ") || text === "/lab_limit" || text.startsWith("/lab_limit ")) {
          const args = text.replace(/^(лаб лимит|\/lab_limit)/, "").trim().split(" ").filter(Boolean);
          const help = `Дневные лимиты (по закрытым сделкам стратегии, день по UTC, считается отдельно по каждой):\n${lab.dayLimitsText()}\n\nЦель по прибыли: «лаб лимит cb4 цель 20» — набрала +$20 за день, до 00:00 UTC не покупает.\nСтоп по убытку: «лаб лимит cb4 убыток 15».\nСнять: «лаб лимит cb4 выкл» (оба) или «лаб лимит cb4 цель выкл». Вместо cb4 можно «все».`;
          if (args.length < 2) await telegram?.send(escapeTelegramHtml(help));
          else {
            const targets = pickCfgs(args[0]);
            const what = args[1];
            const rawV = args[2];
            if (targets.length === 0) await telegram?.send(escapeTelegramHtml(`Не знаю стратегию «${args[0]}». Доступны: ${TRADE_CFGS.map((c) => c.name).join(", ")}, все.`));
            else if (what === "выкл") {
              for (const c of targets) {
                c.goalP = 0;
                c.goalL = 0;
              }
              saveLiveCfg();
              await telegram?.send(escapeTelegramHtml(`✅ Дневные лимиты сняты: ${targets.map((c) => c.name).join(", ")}.\n${lab.dayLimitsText()}`));
            } else if (what === "цель" || what === "убыток") {
              const off = rawV === "выкл" || rawV === "0";
              const v = Number((rawV ?? "").replace(",", "."));
              if (!off && (!Number.isFinite(v) || v <= 0 || v > 100000)) await telegram?.send(escapeTelegramHtml(`Укажи сумму в $ числом, например: лаб лимит ${targets[0].name} ${what} ${what === "цель" ? 20 : 15}`));
              else {
                for (const c of targets) {
                  if (what === "цель") c.goalP = off ? 0 : v;
                  else c.goalL = off ? 0 : v;
                }
                saveLiveCfg();
                await telegram?.send(escapeTelegramHtml(`✅ ${what === "цель" ? "Дневная цель" : "Дневной стоп"} ${off ? "снят" : `$${v}`}: ${targets.map((c) => c.name).join(", ")}. Действует с новых покупок; сегодняшний итог уже учтён.\n${lab.dayLimitsText()}`));
              }
            } else await telegram?.send(escapeTelegramHtml(help));
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

  // v15: на чистом диске (нет тома) — сначала восстановить данные и настройки из закреплённой копии в Telegram, до чтения файлов
  tgbInit(botToken, chatId);
  const restoredTxt = await tgbRestore();
  if (restoredTxt) { console.log(restoredTxt); void telegram?.send(escapeTelegramHtml(restoredTxt)); }

  startPageServer();
  const lab = new ResearchLab();
  if (telegram) lab.onPaperText = (text) => void telegram.send(escapeTelegramHtml(text));

  loadLiveCfg();
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
        try {
          const cr = clob.getApiCreds();
          setPolyboltCreds({ key: cr.key, secret: cr.secret, passphrase: cr.passphrase });
        } catch (e) {
          console.log(`[polybolt] не удалось передать ключи фиду цен: ${(e as Error).message}`);
        }
        lab.liveEnabled = true;
        const t = `🟢 LAB: РЕАЛЬНАЯ торговля включена. ${TRADE_CFGS.filter((c) => c.on).map((c) => `${c.name} ${stakeTxt(c)} (потолок ${c.cap}${c.minBid !== undefined ? `, мин. bid ${c.minBid}` : ""}${notStarted(c) ? `, старт ${c.fromDay} 00:00 UTC` : ""}${ended(c) ? `, закончила ${c.untilDay} 00:00 UTC` : ""})`).join("; ")}. Лимиты: ${LIVE_MAX_PER_WINDOW} в окне, ${LIVE_MAX_OPEN} открытых, ${LIVE_QUOTA_PER_HOUR}/час. Выключить: «лаб торговля выкл».`;
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
  tgbStart();

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
