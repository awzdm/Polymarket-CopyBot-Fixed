/**
 * labAnalytics.ts — данные и сборка страницы-разбора для LAB (v3.3).
 *
 * Что здесь:
 *  1) запись «карточки рынка» (лента цен раз в 5с + все сделки идей с признаками) в файлы lab-data/lab-YYYY-MM-DD.jsonl
 *     (без лимита по времени; файлы старше суток сами сжимаются в .gz);
 *  2) чтение этих файлов и расчёт для каждой сделки показателей до и после входа (по ленте);
 *  3) сборка одной автономной HTML-страницы (все данные внутри, без интернета и библиотек).
 *
 * Модуль ничего не знает о ResearchLab: researchLab.ts вызывает appendMarketRecord() при резолве рынка
 * и buildPage() по команде «лаб страница».
 */
import * as fs from "fs";
import * as path from "path";
import * as zlib from "zlib";
import * as readline from "readline";
import { PAGE_TEMPLATE } from "./labPageTemplate.js";
import { limInfo } from "./labLim.js";

// ───────────────────────── Типы записи ─────────────────────────

/** Строка ленты: [секунд от открытия окна, Up bid, Up ask, Down bid, Down ask, монета bps от открытия (в сторону Up), Binance bps, Coinbase bps] */
export type TapeRow = (number | null)[];

/** Одна сделка идеи (как записал LAB). Идеи-«корзины» (modelb2, bot7b2 …) не пишутся: корзина — это фильтр на странице. */
export interface RawTrade {
  i: string; // идея: model / fair1 / fair2 / bot1..bot7 / margin / cheap / panic / impulse / psolo / temka:<режим>
  c: string; // ячейка (комбинация)
  s: 0 | 1; // 0 = Up, 1 = Down
  ts: number; // мс, момент входа
  px: number; // цена входа (ask)
  sec: number; // секунд до закрытия в момент входа
  bid0?: number | null; // bid в момент входа
  z?: number | null;
  pm?: number | null; // движение монеты от открытия окна в нашу сторону (доля, 0.001 = 0.1%)
  sp10?: number | null;
  sp30?: number | null;
  bin?: number | null; // движение Binance в нашу сторону (доля)
  cb?: number | null; // движение Coinbase в нашу сторону (доля)
  xs?: Record<string, number | null>; // v3.4: Bybit(by)/OKX(ok)/Kraken(kr)/Bitget(bg), доля
  rv2?: number | null;
  rv5?: number | null;
  cg?: number | null;
  cgs?: number | null;
  tr15?: number | null;
  m15?: number | null;
  m60?: number | null;
  q5?: number | null;
  q15?: number | null;
  q60?: number | null;
  fe?: number | null;
  xings?: number | null;
  agreeN?: number | null;
  c05?: number | null;
  c10?: number | null;
  hr?: number | null;
  pk?: number | null; // пик цены токена до входа
  bsz?: number | null;
  asz?: number | null;
  bs10?: number | null; // v5.4: объём лучшего bid / глубина в 5¢ в момент просадки на 10¢ и 20¢ (только fair2)
  dp10?: number | null;
  bs20?: number | null;
  dp20?: number | null;
  hits?: (number | null)[]; // только «Темка»: секунд до +0.02/0.03/0.04/0.05/0.07/0.10 (null — не достигнуто)
}

export interface MarketRec {
  v: 1;
  slug: string;
  coin: string;
  wm: number; // длина окна, мин
  open: number; // мс
  close: number; // мс
  win: 0 | 1; // 0 = Up выиграл
  tape: TapeRow[];
  trades: RawTrade[];
}

// ───────────────────────── Файлы ─────────────────────────

function dayStr(ms: number): string {
  return new Date(ms).toISOString().slice(0, 10);
}

export function appendMarketRecord(dir: string, rec: MarketRec): void {
  try {
    fs.mkdirSync(dir, { recursive: true });
    const f = path.join(dir, `lab-${dayStr(rec.close)}.jsonl`);
    fs.appendFileSync(f, JSON.stringify(rec) + "\n", "utf-8");
  } catch (err) {
    console.error("[lab-data] не удалось записать карточку рынка:", (err as Error).message);
  }
}

/** Сжимает файлы прошлых суток в .gz (по одному, потоком). Безопасно вызывать часто. */
export async function compressOldDays(dir: string): Promise<void> {
  try {
    if (!fs.existsSync(dir)) return;
    const today = dayStr(Date.now());
    for (const name of fs.readdirSync(dir)) {
      const m = name.match(/^lab-(\d{4}-\d{2}-\d{2})\.jsonl$/);
      if (!m || m[1] >= today) continue;
      const src = path.join(dir, name);
      const dst = `${src}.gz`;
      await new Promise<void>((resolve, reject) => {
        const rs = fs.createReadStream(src);
        const gz = zlib.createGzip({ level: 6 });
        const ws = fs.createWriteStream(`${dst}.tmp`);
        rs.on("error", reject);
        gz.on("error", reject);
        ws.on("error", reject);
        ws.on("finish", () => resolve());
        rs.pipe(gz).pipe(ws);
      });
      fs.renameSync(`${dst}.tmp`, dst);
      fs.unlinkSync(src);
    }
  } catch (err) {
    console.error("[lab-data] сжатие старых дней не удалось:", (err as Error).message);
  }
}

async function* readMarketRecords(dir: string, sinceMs: number): AsyncGenerator<MarketRec> {
  if (!fs.existsSync(dir)) return;
  const sinceDay = dayStr(sinceMs);
  const files = fs
    .readdirSync(dir)
    .map((n) => ({ n, m: n.match(/^lab-(\d{4}-\d{2}-\d{2})\.jsonl(\.gz)?$/) }))
    .filter((x) => x.m && x.m[1] >= sinceDay)
    .sort((a, b) => a.n.localeCompare(b.n));
  for (const f of files) {
    const full = path.join(dir, f.n);
    let input: NodeJS.ReadableStream = fs.createReadStream(full);
    if (f.m && f.m[2]) input = input.pipe(zlib.createGunzip());
    const rl = readline.createInterface({ input, crlfDelay: Infinity });
    for await (const line of rl) {
      if (!line) continue;
      try {
        const rec = JSON.parse(line) as MarketRec;
        if (rec.close >= sinceMs) yield rec;
      } catch {
        /* битая строка (обрыв записи) — пропускаем */
      }
    }
  }
}

// ───────────────────────── Колонки ─────────────────────────

export interface ColDef {
  k: string; // ключ
  label: string; // название на странице
  g: "entry" | "pre" | "post" | "tp"; // когда это известно
  d: number; // знаков после запятой
  hint: string; // пояснение
}

/** Идентификаторы в начале строки: ts(сек), монета, сторона, исход(1=выиграл), идея, ячейка, окно(мин), рынок */
export const ID_COLS = ["ts", "coin", "side", "won", "idea", "combo", "wm", "mk"];
export const OFFSETS = [5, 10, 15, 20, 30, 45, 60, 90, 120];
const TP_DELTAS_PAGE = [0.03, 0.05, 0.1, 0.15, 0.2, 0.3];
// v4.4: когда bid впервые опустился на столько ниже цены входа (для симулятора стопа: точный порядок «стоп / цель / добор»)
const DN_DELTAS_PAGE = [0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5];
const TEMKA_IDX: Record<string, number> = { "0.03": 1, "0.05": 3, "0.1": 5 };

/** Дополнительные биржи (ключи как в RawTrade.xs). */
export const XEX = [
  { key: "by", label: "Bybit" },
  { key: "ok", label: "OKX" },
  { key: "kr", label: "Kraken" },
  { key: "bg", label: "Bitget" },
];

export const COLS: ColDef[] = [
  { k: "px", label: "Цена входа (ask)", g: "entry", d: 3, hint: "по какой цене купили" },
  { k: "spr", label: "Спред ask−bid", g: "entry", d: 3, hint: "насколько стакан был широким в момент входа" },
  { k: "sec", label: "Секунд до закрытия", g: "entry", d: 0, hint: "в какой момент окна вошли" },
  { k: "z", label: "Сила z", g: "entry", d: 2, hint: "движение монеты в «сигмах»" },
  { k: "pm", label: "Движение монеты, bps", g: "entry", d: 1, hint: "насколько монета ушла от открытия окна в нашу сторону (100 bps = 1%)" },
  { k: "sp10", label: "Движение за 10с, bps", g: "entry", d: 1, hint: "скорость монеты прямо перед входом" },
  { k: "sp30", label: "Движение за 30с, bps", g: "entry", d: 1, hint: "то же за 30с" },
  { k: "bin", label: "Binance / поток", g: "entry", d: 2, hint: "во сколько раз Binance прошёл от открытия относительно потока бота; меньше 0 — Binance против" },
  { k: "cbr", label: "Coinbase / поток", g: "entry", d: 2, hint: "то же для Coinbase" },
  { k: "bk", label: "Корзина Binance (0-4)", g: "entry", d: 0, hint: "0 против, 1 <50%, 2 50-80%, 3 80-120%, 4 >120%" },
  { k: "ck", label: "Корзина Coinbase (0-4)", g: "entry", d: 0, hint: "то же для Coinbase" },
  ...XEX.flatMap((x) => [
    { k: `xr_${x.key}`, label: `${x.label} / поток`, g: "entry" as const, d: 2, hint: `во сколько раз ${x.label} прошёл от открытия относительно потока; меньше 0 — против` },
    { k: `xb_${x.key}`, label: `Корзина ${x.label} (0-4)`, g: "entry" as const, d: 0, hint: "0 против, 1 <50%, 2 50-80%, 3 80-120%, 4 >120%" },
  ]),
  { k: "nex", label: "Бирж с данными (из всех)", g: "entry", d: 0, hint: "сколько бирж дали цену к моменту входа" },
  { k: "nag", label: "Бирж против", g: "entry", d: 0, hint: "сколько бирж уже по другую сторону от цены открытия (корзина 0)" },
  { k: "ncf", label: "Бирж подтверждают (≥50% потока)", g: "entry", d: 0, hint: "корзины 2-4: биржа прошла в нашу сторону не меньше половины потока" },
  { k: "rv2", label: "Режим волатильности 2м", g: "entry", d: 2, hint: "σ за 2 минуты ÷ обычная σ" },
  { k: "rv5", label: "Режим волатильности 5м", g: "entry", d: 2, hint: "σ за 5 минут ÷ обычная σ" },
  { k: "cg", label: "Соседей с просевшим токеном", g: "entry", d: 0, hint: "заражение: строго" },
  { k: "cgs", label: "Соседей с токеном −0.10 от пика", g: "entry", d: 0, hint: "заражение: мягко" },
  { k: "lo30", label: "Токен: самый низкий bid за первые 30с окна", g: "entry", d: 3, hint: "был ли обвал токена на старте рынка (до входа)" },
  { k: "lo60", label: "Токен: самый низкий bid за первые 60с окна", g: "entry", d: 3, hint: "то же за 60 секунд" },
  { k: "b0", label: "Токен: bid в начале окна", g: "entry", d: 3, hint: "с какой цены токен начал окно (первый замер ленты)" },
  { k: "jmp", label: "Токен до входа: самый резкий скачок вверх за шаг (5с)", g: "entry", d: 3, hint: "«вкат»: маленькое = растёт плавно, большое = были резкие рывки" },
  { k: "rdd", label: "Токен до входа: откат от своего максимума", g: "entry", d: 3, hint: "насколько bid проседал от уже достигнутого максимума; 0 = ни разу не откатывал" },
  { k: "mono", label: "Токен до входа: доля шагов вверх или на месте", g: "entry", d: 2, hint: "1 = рос без единого падения; 0.5 = болтался" },
  { k: "slp", label: "Токен до входа: рост, ¢ в минуту", g: "entry", d: 2, hint: "(bid сейчас − bid в начале) ÷ минут" },
  { k: "rg", label: "Режим монеты: 1 дамп · 2 падение · 3 боковик · 4 рост · 5 памп", g: "entry", d: 0, hint: "что делала монета к моменту входа, независимо от нашей стороны. 5 = резкий памп за последние 30с, 1 = резкий дамп; 4/2 = устойчиво росла/падала на 15-мин свече; 3 = боковик (без явного движения). Пороги подстроены под монету; пусто, если данных нет" },
  { k: "m15", label: "Монета на 15-мин свече: с её начала до входа, bps", g: "entry", d: 1, hint: "в нашу сторону (+) или против (−); пусто, пока не накопилась история" },
  { k: "m60", label: "Монета на часовой свече: с её начала до входа, bps", g: "entry", d: 1, hint: "то же для часа" },
  { k: "q5", label: "Прошлая 5-мин свеча монеты, bps", g: "entry", d: 1, hint: "предыдущие 5 минут: шла в нашу сторону (+) или против (−)" },
  { k: "q15", label: "Прошлая 15-мин свеча монеты, bps", g: "entry", d: 1, hint: "предыдущая 15-минутка целиком, в нашу сторону" },
  { k: "q60", label: "Прошлый час монеты, bps", g: "entry", d: 1, hint: "предыдущий час целиком, в нашу сторону" },
  { k: "tr15", label: "Токен за 15с до входа", g: "entry", d: 3, hint: "ask сейчас минус ask 15с назад" },
  { k: "fe", label: "Edge (справедливая − ask)", g: "entry", d: 3, hint: "p=Φ(d/(σ√t)) минус цена" },
  { k: "xings", label: "Пересечений цены открытия", g: "entry", d: 0, hint: "«пила» за 3 минуты" },
  { k: "agree", label: "Профилей бота с сигналом", g: "entry", d: 0, hint: "согласие профилей" },
  { k: "c05", label: "Других монет в ту же сторону ≥0.05%", g: "entry", d: 0, hint: "подтверждение монетами" },
  { k: "c10", label: "Других монет в ту же сторону ≥0.10%", g: "entry", d: 0, hint: "то же ≥0.10%" },
  { k: "hr", label: "Час суток (UTC)", g: "entry", d: 0, hint: "" },
  { k: "pk", label: "Пик токена до входа", g: "entry", d: 3, hint: "максимальная цена токена в этом окне до входа" },
  { k: "bsz", label: "Объём bid", g: "entry", d: 0, hint: "акций на лучшем bid" },
  { k: "asz", label: "Объём ask", g: "entry", d: 0, hint: "акций на лучшем ask" },
  { k: "tb30", label: "Bid токена: вход − 30с назад", g: "pre", d: 3, hint: "токен до входа рос (+) или падал (−) за 30с" },
  { k: "tb60", label: "Bid токена: вход − 60с назад", g: "pre", d: 3, hint: "то же за 60с" },
  { k: "cmp30", label: "Монета за 30с до входа, bps", g: "pre", d: 1, hint: "ускорялась (+) или тормозила (−) в нашу сторону" },
  ...OFFSETS.map((o) => ({ k: `d${o}`, label: `Bid через ${o}с после входа − цена входа`, g: "post" as const, d: 3, hint: "если бы продали по bid через столько секунд" })),
  { k: "mfe", label: "Лучший bid после входа − цена", g: "post", d: 3, hint: "максимальная прибыль на бумаге" },
  { k: "mae", label: "Худший bid после входа − цена", g: "post", d: 3, hint: "максимальная просадка" },
  { k: "tmae", label: "Секунд до худшей точки", g: "post", d: 0, hint: "" },
  { k: "lost10", label: "Просел на 0.10+ после входа (0/1)", g: "post", d: 0, hint: "bid опускался на 10 центов и больше от цены входа" },
  { k: "flip", label: "Токен падал ниже 0.50 (0/1)", g: "post", d: 0, hint: "потерял лидерство после входа (только если вход был выше 0.55)" },
  { k: "end10", label: "Bid за 10с до конца − цена входа", g: "post", d: 3, hint: "состояние перед самым закрытием" },
  { k: "cm10", label: "Монета за 10с после входа, bps", g: "post", d: 1, hint: "продолжила (+) или откатила (−)" },
  { k: "cm30", label: "Монета за 30с после входа, bps", g: "post", d: 1, hint: "" },
  { k: "fin", label: "Итоговый отрыв монеты, bps", g: "post", d: 1, hint: "на какой отметке закрылась монета в нашу сторону" },
  { k: "finr", label: "Итоговый отрыв ÷ отрыв на входе", g: "post", d: 2, hint: "1 — отрыв не изменился; меньше 0 — перешла на другую сторону" },
  // v4.5: «свеча» окна по монете (в нашу сторону, bps): что было от открытия окна до входа и весь рынок целиком
  { k: "chi", label: "Свеча до входа: максимум монеты, bps", g: "entry", d: 1, hint: "на сколько монета максимум уходила в нашу сторону от открытия окна до входа" },
  { k: "clo", label: "Свеча до входа: минимум монеты, bps", g: "entry", d: 1, hint: "на сколько максимум уходила против нас до входа (≤ 0)" },
  { k: "crg", label: "Свеча до входа: размах, bps", g: "entry", d: 1, hint: "максимум − минимум; маленький размах = «доджи», монета топталась у открытия" },
  { k: "cbd", label: "Свеча до входа: тело ÷ размах", g: "entry", d: 2, hint: "1 — шла одним движением; около 0 — доджи (вернулась к открытию); отрицательное — сейчас против" },
  { k: "whi", label: "Вся свеча окна: максимум монеты, bps", g: "post", d: 1, hint: "задним числом: высшая точка монеты за окно в нашу сторону" },
  { k: "wlo", label: "Вся свеча окна: минимум монеты, bps", g: "post", d: 1, hint: "задним числом: низшая точка монеты за окно (≤ 0)" },
  { k: "wrg", label: "Вся свеча окна: размах, bps", g: "post", d: 1, hint: "задним числом: максимум − минимум за окно" },
  { k: "wbd", label: "Вся свеча окна: тело ÷ размах", g: "post", d: 2, hint: "задним числом: итоговый отрыв ÷ размах всей свечи" },
  { k: "wup", label: "Вся свеча окна: тень в нашу сторону, bps", g: "post", d: 1, hint: "задним числом: насколько монета уходила дальше итога в нашу сторону и вернулась" },
  { k: "tmfe", label: "Секунд до лучшей точки", g: "post", d: 0, hint: "когда bid был на максимуме после входа" },
  { k: "mnp", label: "Худший bid после лучшей точки (от входа)", g: "post", d: 3, hint: "насколько bid упал от входа ПОСЛЕ своего максимума" },
  { k: "wdn", label: "Вся свеча окна: тень против нас, bps", g: "post", d: 1, hint: "задним числом: насколько монета уходила дальше итога против нас и вернулась" },
  ...TP_DELTAS_PAGE.map((d) => ({ k: `tp${Math.round(d * 100)}`, label: `Секунд до bid ≥ вход+${d.toFixed(2)}`, g: "tp" as const, d: 0, hint: "пусто — цель не достигнута" })),
  ...DN_DELTAS_PAGE.map((d) => ({ k: `dn${Math.round(d * 100)}`, label: `Секунд до bid ≤ вход−${d.toFixed(2)}`, g: "tp" as const, d: 0, hint: "пусто — просадки такой глубины не было" })),
  // v5.4: состояние в момент, когда bid впервые упал на 10 / 20 центов от входа (для разбора стопов; задним числом — не фильтр входа)
  { k: "bs10", label: "Объём bid в момент просадки −10¢", g: "post", d: 0, hint: "акций на лучшем bid, когда токен упал на 10¢ от входа (пусто — не записано)" },
  { k: "dp10", label: "Глубина bid (5¢) в момент просадки −10¢", g: "post", d: 0, hint: "акций в 5 центах от лучшего bid" },
  { k: "bs20", label: "Объём bid в момент просадки −20¢", g: "post", d: 0, hint: "акций на лучшем bid, когда токен упал на 20¢ от входа" },
  { k: "dp20", label: "Глубина bid (5¢) в момент просадки −20¢", g: "post", d: 0, hint: "акций в 5 центах от лучшего bid" },
  ...[10, 20].flatMap((c) => [
    { k: `fs${c}`, label: `Падение bid за 5с до просадки −${c}¢`, g: "post" as const, d: 3, hint: "на сколько bid упал за 5с до момента, когда он достиг вход−" + (c / 100).toFixed(2) + " (большое = быстрый провал)" },
    { k: `cx${c}`, label: `Монета за 10с до просадки −${c}¢, bps`, g: "post" as const, d: 1, hint: "движение монеты в нашу сторону за 10с до момента просадки; минус = монета шла против нас" },
    { k: `cy${c}`, label: `Монета в момент просадки −${c}¢ (от входа), bps`, g: "post" as const, d: 1, hint: "где монета относительно момента входа; минус = ушла против нас" },
  ]),
];

const COL_IDX: Record<string, number> = {};
COLS.forEach((c, i) => (COL_IDX[c.k] = ID_COLS.length + i));

// ───────────────────────── Расчёт показателей сделки ─────────────────────────

function rnd(x: number | null | undefined, d: number): number | null {
  if (x === null || x === undefined || !Number.isFinite(x)) return null;
  const f = Math.pow(10, d);
  return Math.round(x * f) / f;
}

/** множитель порогов режима монеты (BTC=1, остальные волатильнее) */
const REG_SCALE: Record<string, number> = { Bitcoin: 1, Ethereum: 1.4, Solana: 1.8, XRP: 1.8, Dogecoin: 2.2, _: 1.5 };

function binBucket(r: number): number {
  return r < 0 ? 0 : r < 0.5 ? 1 : r < 0.8 ? 2 : r < 1.2 ? 3 : 4;
}

interface Series {
  t: number[];
  bid: (number | null)[];
  ask: (number | null)[];
  coin: (number | null)[]; // bps в нашу сторону
  bin: (number | null)[];
  cb: (number | null)[];
}

function seriesFor(m: MarketRec, side: 0 | 1): Series {
  const s: Series = { t: [], bid: [], ask: [], coin: [], bin: [], cb: [] };
  const sg = side === 0 ? 1 : -1;
  for (const r of m.tape) {
    s.t.push(r[0] as number);
    s.bid.push(side === 0 ? r[1] : r[3]);
    s.ask.push(side === 0 ? r[2] : r[4]);
    s.coin.push(r[5] === null || r[5] === undefined ? null : sg * (r[5] as number));
    s.bin.push(r[6] === null || r[6] === undefined ? null : sg * (r[6] as number));
    s.cb.push(r[7] === null || r[7] === undefined ? null : sg * (r[7] as number));
  }
  return s;
}

/** последнее известное значение на момент time (не старше maxAge секунд) */
function valAt(t: number[], arr: (number | null)[], time: number, maxAge = 12): number | null {
  let lo = 0;
  let hi = t.length - 1;
  let idx = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (t[mid] <= time) {
      idx = mid;
      lo = mid + 1;
    } else hi = mid - 1;
  }
  for (let i = idx; i >= 0 && time - t[i] <= maxAge; i--) {
    const v = arr[i];
    if (v !== null && v !== undefined) return v;
  }
  return null;
}

/** Строка для страницы: идентификаторы + показатели в порядке COLS. */
export function computeRow(
  m: MarketRec,
  tr: RawTrade,
  mkId: number,
  coinIdx: number,
  ideaIdx: number,
  comboIdx: number,
  series: [Series | null, Series | null],
): (number | null)[] {
  const won = tr.s === m.win ? 1 : 0;
  const dur = (m.close - m.open) / 1000;
  const entryT = (tr.ts - m.open) / 1000;
  const px = tr.px;
  const se = (series[tr.s] ??= seriesFor(m, tr.s));
  const out: (number | null)[] = new Array(ID_COLS.length + COLS.length).fill(null);
  let candleW: { hiW: number; loW: number } | null = null;
  out[0] = Math.round(tr.ts / 1000);
  out[1] = coinIdx;
  out[2] = tr.s;
  out[3] = won;
  out[4] = ideaIdx;
  out[5] = comboIdx;
  out[6] = m.wm;
  out[7] = mkId;
  const set = (k: string, v: number | null | undefined, d: number): void => {
    out[COL_IDX[k]] = rnd(v ?? null, d);
  };

  set("px", px, 3);
  set("spr", tr.bid0 !== null && tr.bid0 !== undefined ? px - tr.bid0 : null, 3);
  set("sec", tr.sec, 0);
  set("z", tr.z, 2);
  const pmBps = tr.pm !== null && tr.pm !== undefined ? tr.pm * 1e4 : null;
  set("pm", pmBps, 1);
  set("sp10", tr.sp10 !== null && tr.sp10 !== undefined ? tr.sp10 * 1e4 : null, 1);
  set("sp30", tr.sp30 !== null && tr.sp30 !== undefined ? tr.sp30 * 1e4 : null, 1);
  if (tr.bin !== null && tr.bin !== undefined && tr.pm && tr.pm > 0) {
    const r = tr.bin / tr.pm;
    set("bin", r, 2);
    set("bk", binBucket(r), 0);
  }
  if (tr.cb !== null && tr.cb !== undefined && tr.pm && tr.pm > 0) {
    const r = tr.cb / tr.pm;
    set("cbr", r, 2);
    set("ck", binBucket(r), 0);
  }
  // консенсус бирж: Binance, Coinbase и остальные
  const bks: number[] = [];
  if (tr.pm && tr.pm > 0) {
    if (tr.bin !== null && tr.bin !== undefined) bks.push(binBucket(tr.bin / tr.pm));
    if (tr.cb !== null && tr.cb !== undefined) bks.push(binBucket(tr.cb / tr.pm));
    for (const x of XEX) {
      const v = tr.xs ? tr.xs[x.key] : null;
      if (v !== null && v !== undefined) {
        const r = v / tr.pm;
        set(`xr_${x.key}`, r, 2);
        set(`xb_${x.key}`, binBucket(r), 0);
        bks.push(binBucket(r));
      }
    }
    set("nex", bks.length, 0);
    set("nag", bks.filter((b) => b === 0).length, 0);
    set("ncf", bks.filter((b) => b >= 2).length, 0);
  }
  set("rv2", tr.rv2, 2);
  set("rv5", tr.rv5, 2);
  set("cg", tr.cg, 0);
  set("cgs", tr.cgs, 0);
  set("tr15", tr.tr15, 3);
  set("m15", tr.m15, 1);
  {
    // режим монеты (в абсолютном направлении, не «в нашу сторону»): дамп/падение/боковик/рост/памп
    const sg = tr.s === 0 ? 1 : -1;
    const k = REG_SCALE[m.coin] ?? REG_SCALE._ ?? 1.5;
    const sp30 = tr.sp30 !== null && tr.sp30 !== undefined ? tr.sp30 * 1e4 * sg : null;
    const trendRaw = tr.m15 ?? tr.m60 ?? pmBps;
    const trend = trendRaw !== null && trendRaw !== undefined ? trendRaw * sg : null;
    let rg: number | null = null;
    if (sp30 !== null && sp30 >= 6 * k) rg = 5;
    else if (sp30 !== null && sp30 <= -6 * k) rg = 1;
    else if (trend !== null) rg = trend >= 8 * k ? 4 : trend <= -8 * k ? 2 : 3;
    else if (sp30 !== null) rg = 3;
    set("rg", rg, 0);
  }
  set("m60", tr.m60, 1);
  set("q5", tr.q5, 1);
  set("q15", tr.q15, 1);
  set("q60", tr.q60, 1);
  set("fe", tr.fe, 3);
  set("xings", tr.xings, 0);
  set("agree", tr.agreeN, 0);
  set("c05", tr.c05, 0);
  set("c10", tr.c10, 0);
  set("hr", tr.hr, 0);
  set("pk", tr.pk, 3);
  set("bsz", tr.bsz, 0);
  set("bs10", tr.bs10, 0);
  set("dp10", tr.dp10, 0);
  set("bs20", tr.bs20, 0);
  set("dp20", tr.dp20, 0);
  set("asz", tr.asz, 0);

  // до входа (по ленте)
  const bidNow = tr.bid0 ?? valAt(se.t, se.bid, entryT);
  if (bidNow !== null && bidNow !== undefined) {
    const b30 = valAt(se.t, se.bid, entryT - 30);
    const b60 = valAt(se.t, se.bid, entryT - 60);
    if (b30 !== null && entryT - 30 >= -1) set("tb30", bidNow - b30, 3);
    if (b60 !== null && entryT - 60 >= -1) set("tb60", bidNow - b60, 3);
  }
  // плавность роста токена от начала окна до входа (по ленте 5с)
  {
    let prev: number | null = null, first: number | null = null, mx = -1, maxJ = 0, maxDd = 0, up = 0, steps = 0, firstT = 0;
    for (let i = 0; i < se.t.length; i++) {
      if (se.t[i] > entryT) break;
      const b = se.bid[i];
      if (b === null || b === undefined) continue;
      if (first === null) { first = b; firstT = se.t[i]; }
      if (prev !== null) {
        steps++;
        if (b >= prev - 1e-9) up++;
        if (b - prev > maxJ) maxJ = b - prev;
      }
      if (b > mx) mx = b;
      if (mx - b > maxDd) maxDd = mx - b;
      prev = b;
    }
    if (first !== null && steps >= 3) {
      set("b0", first, 3);
      {
        let l30 = Infinity, l60 = Infinity;
        for (let i = 0; i < se.t.length; i++) {
          if (se.t[i] > entryT || se.t[i] > 60) break;
          const b = se.bid[i];
          if (b === null || b === undefined) continue;
          if (b < l60) l60 = b;
          if (se.t[i] <= 30 && b < l30) l30 = b;
        }
        if (l30 < Infinity) set("lo30", l30, 3);
        if (l60 < Infinity) set("lo60", l60, 3);
      }
      set("jmp", maxJ, 3);
      set("rdd", maxDd, 3);
      set("mono", up / steps, 2);
      const mins = (entryT - firstT) / 60;
      if (prev !== null && mins >= 0.3) set("slp", ((prev - first) * 100) / mins, 2);
    }
  }

  const c0 = pmBps ?? valAt(se.t, se.coin, entryT);
  if (c0 !== null && c0 !== undefined && entryT - 30 >= -1) {
    const cPrev = valAt(se.t, se.coin, entryT - 30);
    if (cPrev !== null) set("cmp30", c0 - cPrev, 1);
  }

  // v4.5: свеча по монете до входа и за всё окно
  {
    let hi = 0, lo = 0, hiW = 0, loW = 0;
    for (let i = 0; i < se.t.length; i++) {
      const v = se.coin[i];
      if (v === null || v === undefined) continue;
      if (se.t[i] <= entryT) { if (v > hi) hi = v; if (v < lo) lo = v; }
      if (v > hiW) hiW = v;
      if (v < loW) loW = v;
    }
    if (c0 !== null && c0 !== undefined) { hi = Math.max(hi, c0); lo = Math.min(lo, c0); }
    if (se.t.length > 3) {
      set("chi", hi, 1);
      set("clo", lo, 1);
      set("crg", hi - lo, 1);
      if (c0 !== null && c0 !== undefined && hi - lo > 0.05) set("cbd", c0 / (hi - lo), 2);
      candleW = { hiW, loW };
    }
  }

  // после входа
  for (const o of OFFSETS) {
    const target = entryT + o;
    if (target > dur + 2) continue;
    const v = valAt(se.t, se.bid, target);
    if (v !== null) set(`d${o}`, v - px, 3);
  }
  let mx = -Infinity;
  let mn = Infinity;
  let tmn = 0;
  let tmx = 0;
  let mnAfter = Infinity;
  let first10: number | null = null;
  let fell50 = false;
  let seen = 0;
  const tpT: (number | null)[] = TP_DELTAS_PAGE.map(() => null);
  const dnT: (number | null)[] = DN_DELTAS_PAGE.map(() => null);
  for (let i = 0; i < se.t.length; i++) {
    if (se.t[i] <= entryT) continue;
    const b = se.bid[i];
    if (b === null || b === undefined) continue;
    seen++;
    const dlt = b - px;
    if (dlt > mx) {
      mx = dlt;
      tmx = se.t[i] - entryT;
      mnAfter = dlt;
    } else if (dlt < mnAfter) mnAfter = dlt;
    if (dlt < mn) {
      mn = dlt;
      tmn = se.t[i] - entryT;
    }
    if (first10 === null && dlt <= -0.1 + 1e-9) first10 = se.t[i] - entryT;
    if (b < 0.5) fell50 = true;
    TP_DELTAS_PAGE.forEach((d, j) => {
      if (tpT[j] === null && dlt >= d - 1e-9) tpT[j] = se.t[i] - entryT;
    });
    DN_DELTAS_PAGE.forEach((d, j) => {
      if (dnT[j] === null && dlt <= -d + 1e-9) dnT[j] = se.t[i] - entryT;
    });
  }
  if (seen > 0) {
    set("mfe", mx, 3);
    set("tmfe", tmx, 0);
    set("mnp", mnAfter, 3);
    set("mae", mn, 3);
    set("tmae", tmn, 0);
    set("lost10", first10 !== null ? 1 : 0, 0);
    if (px >= 0.55) set("flip", fell50 ? 1 : 0, 0);
  }
  // v5.4: состояние в момент просадки на 10 / 20 центов
  [10, 20].forEach((c) => {
    const j = DN_DELTAS_PAGE.findIndex((d) => Math.round(d * 100) === c);
    const dtc = j >= 0 ? dnT[j] : null;
    if (dtc === null || dtc === undefined) return;
    const tc = entryT + dtc;
    const bNow = valAt(se.t, se.bid, tc);
    const bPrev = valAt(se.t, se.bid, tc - 5);
    if (bNow !== null && bPrev !== null) set(`fs${c}`, bPrev - bNow, 3);
    const cNow = valAt(se.t, se.coin, tc);
    const cPrev = valAt(se.t, se.coin, tc - 10);
    if (cNow !== null && cPrev !== null) set(`cx${c}`, cNow - cPrev, 1);
    if (cNow !== null && c0 !== null && c0 !== undefined) set(`cy${c}`, cNow - c0, 1);
  });
  if (entryT < dur - 10) {
    const e10 = valAt(se.t, se.bid, dur - 10);
    if (e10 !== null) set("end10", e10 - px, 3);
  }
  if (c0 !== null && c0 !== undefined) {
    if (entryT + 10 <= dur + 2) {
      const v = valAt(se.t, se.coin, entryT + 10);
      if (v !== null) set("cm10", v - c0, 1);
    }
    if (entryT + 30 <= dur + 2) {
      const v = valAt(se.t, se.coin, entryT + 30);
      if (v !== null) set("cm30", v - c0, 1);
    }
  }
  const fin = valAt(se.t, se.coin, dur + 1, 15);
  if (fin !== null) {
    set("fin", fin, 1);
    if (pmBps !== null && pmBps > 0.05) set("finr", fin / pmBps, 2);
    if (candleW) {
      const hiW = Math.max(candleW.hiW, fin, 0), loW = Math.min(candleW.loW, fin, 0), rg = hiW - loW;
      set("whi", hiW, 1);
      set("wlo", loW, 1);
      set("wrg", rg, 1);
      if (rg > 0.05) set("wbd", fin / rg, 2);
      set("wup", hiW - Math.max(0, fin), 1);
      set("wdn", Math.min(0, fin) - loW, 1);
    }
  }
  TP_DELTAS_PAGE.forEach((d, j) => {
    let v = tpT[j];
    if (tr.hits) {
      const hi = TEMKA_IDX[String(d)];
      const hv = tr.hits[hi];
      if (hv !== undefined) v = hv; // для «Темки» время точнее (по живому стакану)
    }
    set(`tp${Math.round(d * 100)}`, v, 0);
  });
  DN_DELTAS_PAGE.forEach((d, j) => set(`dn${Math.round(d * 100)}`, dnT[j], 0));
  return out;
}

// ───────────────────────── Сборка набора данных ─────────────────────────

export interface PageOptions {
  sinceMs: number; // с какого момента брать рынки
  periodLabel: string;
  coin?: string; // «Bitcoin» …, если нужна одна монета
  wm?: number; // 5 или 15
  maxRows: number; // потолок числа строк в файле (выше — равномерная выборка внутри каждой ячейки)
}

/** v4.7: компактное хранилище строк по столбцам (Float32; время — Float64). Вместо миллионов JS-массивов — в разы меньше памяти. */
export class ColStore {
  n = 0;
  cap = 0;
  cols: (Float32Array | Float64Array)[] = [];
  constructor(public nc: number) {
    for (let j = 0; j < nc; j++) this.cols.push(j === 0 ? new Float64Array(0) : new Float32Array(0));
  }
  push(row: (number | null)[]): number {
    if (this.n === this.cap) {
      const nc2 = Math.max(1 << 14, this.cap * 2);
      for (let j = 0; j < this.nc; j++) {
        const old = this.cols[j];
        const a = j === 0 ? new Float64Array(nc2) : new Float32Array(nc2);
        a.set(old);
        this.cols[j] = a;
      }
      this.cap = nc2;
    }
    const i = this.n++;
    for (let j = 0; j < this.nc; j++) {
      const v = row[j];
      this.cols[j][i] = v === null || v === undefined ? NaN : v;
    }
    return i;
  }
  get(j: number, i: number): number | null {
    const v = this.cols[j][i];
    return v !== v ? null : v;
  }
  row(i: number): (number | null)[] {
    const r: (number | null)[] = new Array(this.nc);
    for (let j = 0; j < this.nc; j++) r[j] = this.get(j, i);
    return r;
  }
}
const COL_DEC: number[] = [...ID_COLS.map(() => 0), ...COLS.map((c) => c.d)];

export interface PageData {
  store?: ColStore; // v4.7: строки лежат тут (rows при этом пустой)
  sel?: number[]; // индексы строк store, которые идут в файл
  meta: Record<string, unknown>;
  coins: string[];
  ideas: string[];
  combos: string[];
  cols: ColDef[];
  idCols: string[];
  offsets: number[];
  rows: (number | null)[][];
  exchanges: { label: string; b: string; r: string }[];
  pm?: Record<string, [number, number, string, string]>; // v5.5: путь bid и ask рынка (шаг 2с) для ВСЕХ ТС: ключ рынок.сторона → [открытие (с), стартовый индекс, строка bid, строка ask]; символ = 256+центы, 255 = нет данных
  paths?: Record<string, (number | null)[]>; // v4.4: путь bid по секундам (шаг 2с, в центах) для сделок «торговых» ячеек; ключ = рынок.сторона.время входа
  lim?: Record<string, unknown>; // v6: сетка «лимитки на прострелы»
  paperLim?: unknown; // v6: бумажный тест лимиток по живому стакану (lab-lim.json от бота)
  cells: Record<string, number[]>; // "idea|combo" -> [n, wins, sumRoi, sumPx, nMarkets] — по ВСЕМ сделкам (до выборки)
}

function mulberry(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// v4.4: для каких ячеек («идея|ячейка») писать подробный путь цены по каждой сделке (задаёт бот: ячейки, которые он торгует)
let PATH_CELLS = new Set<string>();
export function setPathCells(keys: string[]): void {
  PATH_CELLS = new Set(keys);
}
const PATH_STEP = 2;
/** v5.5: путь рынка (bid и ask стороны) шагом 2с, от самого раннего входа в этом рынке до конца. Общий для всех ТС. */
function pathMk(m: MarketRec, side: 0 | 1, minE: number, series: [Series | null, Series | null]): [number, number, string, string] | null {
  const se = (series[side] ??= seriesFor(m, side));
  const dur = (m.close - m.open) / 1000;
  if (!(dur > minE)) return null;
  const st = Math.max(0, Math.floor(minE / 2));
  const bb: string[] = [];
  const aa: string[] = [];
  let any = false;
  for (let t = st * PATH_STEP; t <= dur + 0.5; t += PATH_STEP) {
    const vb = valAt(se.t, se.bid, t, 6);
    const va = valAt(se.t, se.ask, t, 6);
    if (vb !== null) any = true;
    bb.push(String.fromCharCode(vb === null ? 255 : 256 + Math.max(0, Math.min(100, Math.round(vb * 100)))));
    aa.push(String.fromCharCode(va === null ? 255 : 256 + Math.max(0, Math.min(100, Math.round(va * 100)))));
  }
  return any ? [Math.round(m.open / 1000), st, bb.join(""), aa.join("")] : null;
}
function pathOf(m: MarketRec, tr: RawTrade, series: [Series | null, Series | null]): (number | null)[] | null {
  const se = (series[tr.s] ??= seriesFor(m, tr.s));
  const dur = (m.close - m.open) / 1000;
  const entryT = (tr.ts - m.open) / 1000;
  if (!(dur > entryT)) return null;
  const out: (number | null)[] = [];
  for (let t = entryT; t <= dur + 0.5; t += PATH_STEP) {
    const v = valAt(se.t, se.bid, t, 6);
    out.push(v === null ? null : Math.round(v * 100));
  }
  return out.some((x) => x !== null) ? out : null;
}
// ───────────────────────── v6: лимитки на прострелы ─────────────────────────
// Ставим лимитную покупку токена ниже рынка на цену L, ждём, пока цена туда упадёт; после покупки продаём лимиткой на L+Δ или держим до резолва.
// Два варианта замера: "s5" — по bid в момент замера ленты (раз в 5с, красивее: ловит только то, что видно в замере),
// "wk" — по минимуму/максимуму bid между замерами (честнее, видит прострелы; только для новых данных).
const LIM_L = [0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.94, 0.95, 0.96, 0.97, 0.98];
const LIM_X = [2, 3, 5, 8, 12, 0]; // цель в центах; 0 = держим до резолва
const LIM_W = [300, 120, 60, 30]; // ордер стоит только в последние N секунд рынка (300 = весь рынок)
const LIM_V = ["s5", "wk"] as const;
// режимы 2 и 3: динамическая лимитка (ходит за ценой) и «обвал на старте». Выходы: >0 лимитка на +X¢, <0 продать по рынку через |X| с, 0 держать до резолва
const LIM_E = [2, 3, 5, 8, 12, -5, -10, -30, 0];
const DYN_D = [5, 10, 20, 30]; // насколько ниже цены висит лимитка, %
const DYN_U = [1, 3]; // как часто лимитка подтягивается за ценой: каждый замер (5с) или каждые 3 (15с)
const DYN_B = [0, 1]; // 0 — любая цена токена, 1 — только 15-85¢
const DYN_W = [60, 120, 999]; // ордер живёт первые N секунд рынка (999 = весь рынок)
const CRA_S = [15, 30, 60, 120]; // окно начала рынка, секунд
const CRA_P = [1, 2, 3, 5, 7, 10, 15, 20]; // до какой цены (¢) должен упасть токен
const FILL_KEEP = 20;
interface LimCell { n: number; f: number; h: number; w: number; pnl: number; cost: number; d: Map<string, number>; q: Map<number, number> }
class LimAgg {
  cells = new Map<string, LimCell>();
  // примеры реальных сделок для проверки глазами: до FILL_KEEP случайных исполнений на каждый вариант (ключ без выхода)
  fills = new Map<string, { cnt: number; pick: { mk: string; ft: number; pay: number; ex: number[] }[] }>();
  mkt = new Map<string, { o: number; c: string; w: number; sd: number; win: number; t: number[]; b: (number | null)[]; lo: (number | null)[] | null; hi: (number | null)[] | null }>();
  private rnd = mulberry(4242);
  private cur: { m: MarketRec; side: number; t: number[]; bid: (number | null)[]; mn: (number | null)[]; mx: (number | null)[]; wk: boolean } | null = null;
  days = new Set<string>();
  markets = 0;
  wkMarkets = 0;
  private recordFill(key: string, fi: number, pay: number, ex: number[]): void {
    const c = this.cur;
    if (!c) return;
    let f = this.fills.get(key);
    if (!f) { f = { cnt: 0, pick: [] }; this.fills.set(key, f); }
    f.cnt++;
    const mk = `${c.m.slug}.${c.side}`;
    const item = { mk, ft: c.t[fi], pay: Math.round(pay * 1000) / 10, ex };
    if (f.pick.length < FILL_KEEP) f.pick.push(item);
    else { const j = Math.floor(this.rnd() * f.cnt); if (j < FILL_KEEP) f.pick[j] = item; else return; }
    if (!this.mkt.has(mk)) {
      const cv = (a: (number | null)[]): (number | null)[] => a.map((v) => (v === null ? null : Math.round(v * 100)));
      this.mkt.set(mk, { o: Math.round(c.m.open / 1000), c: c.m.coin, w: c.m.wm, sd: c.side, win: c.m.win, t: c.t.map((x) => Math.round(x)), b: cv(c.bid), lo: c.wk ? cv(c.mn) : null, hi: c.wk ? cv(c.mx) : null });
    }
  }
  // 4-часовые отрезки (по закрытию рынка): из них на странице строится кумулята варианта
  private addQ(c: LimCell, pnl: number): void {
    if (!this.cur) return;
    const b = Math.floor(this.cur.m.close / 14400000);
    c.q.set(b, (c.q.get(b) ?? 0) + pnl);
  }
  private cell(k: string): LimCell {
    let c = this.cells.get(k);
    if (!c) { c = { n: 0, f: 0, h: 0, w: 0, pnl: 0, cost: 0, d: new Map(), q: new Map() }; this.cells.set(k, c); }
    return c;
  }
  add(m: MarketRec): void {
    if (!m.tape || m.tape.length < 20) return;
    const day = dayStr(m.close);
    const dur = (m.close - m.open) / 1000;
    this.days.add(day);
    this.markets++;
    const hasWk = m.tape.some((r) => r.length >= 12 && r[8] !== null && r[8] !== undefined);
    if (hasWk) this.wkMarkets++;
    for (const side of [0, 1] as const) {
      const n = m.tape.length;
      const t: number[] = new Array(n), bid: (number | null)[] = new Array(n), mn: (number | null)[] = new Array(n), mx: (number | null)[] = new Array(n);
      for (let i = 0; i < n; i++) {
        const r = m.tape[i];
        t[i] = r[0] as number;
        bid[i] = (side === 0 ? r[1] : r[3]) ?? null;
        mn[i] = r.length >= 12 ? ((side === 0 ? r[8] : r[10]) ?? null) : null;
        mx[i] = r.length >= 12 ? ((side === 0 ? r[9] : r[11]) ?? null) : null;
      }
      const won = side === m.win;
      this.cur = { m, side, t, bid, mn, mx, wk: hasWk };
      for (const v of LIM_V) {
        if (v === "wk" && !hasWk) continue;
        this.addDynCrash(v, t, bid, mn, mx, dur, won, day);
        for (const W of LIM_W) {
          // строка, на которой ордер ставится: первая с t ≥ 10с и ≤ W секунд до конца
          let i0 = -1;
          for (let i = 0; i < n; i++) { if (t[i] >= 10 && dur - t[i] <= W && dur - t[i] >= 3) { i0 = i; break; } }
          if (i0 < 0) continue;
          const b0 = bid[i0];
          if (b0 === null) continue;
          for (const L of LIM_L) {
            const base = `${v}|${W}|${L}|`;
            for (const X of LIM_X) this.cell(base + X).n++;
            if (b0 <= L + 1e-9) continue; // ордер должен стоять ниже рынка
            let fi = -1;
            for (let i = i0 + 1; i < n; i++) {
              if (dur - t[i] < 3) break;
              const lowV = v === "s5" ? bid[i] : mn[i];
              if (lowV !== null && lowV <= (v === "s5" ? L : L - 0.01) + 1e-9) { fi = i; break; }
            }
            if (fi < 0) continue;
            // после покупки: когда впервые достигнуты цели
            const hit: (number | null)[] = LIM_X.map(() => null);
            let runMax = -1;
            for (let j = fi + 1; j < n; j++) {
              const hv = v === "s5" ? bid[j] : mx[j];
              if (hv === null) continue;
              if (hv > runMax) runMax = hv;
              for (let k = 0; k < LIM_X.length; k++) {
                const X = LIM_X[k];
                if (X > 0 && hit[k] === null && runMax >= L + X / 100 - 1e-9) hit[k] = t[j];
              }
            }
            const exS: number[] = [];
            LIM_X.forEach((X, k) => {
              const c = this.cell(base + X);
              c.f++;
              let pnl: number;
              if (X > 0 && hit[k] !== null) { c.h++; pnl = X / 100; }
              else pnl = won ? 1 - L : -L;
              if (won) c.w++;
              c.pnl += pnl;
              c.cost += L;
              c.d.set(day, (c.d.get(day) ?? 0) + pnl);
              this.addQ(c, pnl);
              exS.push(X > 0 && hit[k] !== null ? (hit[k] as number) : -1, Math.round(pnl * 1000) / 10);
            });
            this.recordFill(base.slice(0, -1), fi, L, exS);
          }
        }
      }
    }
  }
  /** Общая часть: после покупки по цене pay на замере fi считаем все выходы. */
  private finish(base: string, v: string, fi: number, pay: number, t: number[], bid: (number | null)[], mx: (number | null)[], dur: number, won: boolean, day: string): void {
    const n = t.length;
    const ex: number[] = [];
    for (const X of LIM_E) {
      const c = this.cell(base + X);
      c.f++;
      let pnl: number | null = null;
      let exT = -1;
      if (X > 0) {
        const goal = pay + X / 100 - 1e-9;
        for (let j = fi + 1; j < n; j++) {
          const hv = v === "s5" ? bid[j] : mx[j];
          if (hv !== null && hv >= goal) { pnl = X / 100; exT = t[j]; break; }
        }
      } else if (X < 0) {
        const tt = t[fi] - X;
        for (let j = fi + 1; j < n; j++) {
          if (t[j] >= tt && bid[j] !== null) { if (dur - t[j] >= 1) { pnl = (bid[j] as number) - pay; exT = t[j]; } break; }
        }
      }
      if (pnl !== null) c.h++;
      else pnl = won ? 1 - pay : -pay;
      if (won) c.w++;
      c.pnl += pnl;
      c.cost += pay;
      c.d.set(day, (c.d.get(day) ?? 0) + pnl);
      this.addQ(c, pnl);
      ex.push(exT, Math.round(pnl * 1000) / 10);
    }
    this.recordFill(base.slice(0, -1), fi, pay, ex);
  }
  private addDynCrash(v: string, t: number[], bid: (number | null)[], mn: (number | null)[], mx: (number | null)[], dur: number, won: boolean, day: string): void {
    const n = t.length;
    // динамическая лимитка
    for (const D of DYN_D) for (const U of DYN_U) for (const B of DYN_B) for (const Wd of DYN_W) {
      const base = `d|${v}|${D}|${U}|${B}|${Wd}|`;
      for (const X of LIM_E) this.cell(base + X).n++;
      let lim: number | null = null, fi = -1, pay = 0;
      for (let k = 0; k < n - 1; k++) {
        if (k % U === 0) {
          const b = bid[k];
          lim = b !== null && b > 0 && (B === 0 || (b >= 0.15 && b <= 0.85)) ? b * (1 - D / 100) : null;
        }
        if (t[k + 1] > Wd || dur - t[k + 1] < 3) break;
        if (lim === null || lim < 0.02) continue;
        const low = v === "s5" ? bid[k + 1] : mn[k + 1];
        if (low !== null && low <= (v === "s5" ? lim : lim - 0.01) + 1e-9) { fi = k + 1; pay = lim; break; }
      }
      if (fi >= 0) this.finish(base, v, fi, Math.round(pay * 1000) / 1000, t, bid, mx, dur, won, day);
    }
    // обвал на старте
    for (const S of CRA_S) for (const P of CRA_P) {
      const base = `c|${v}|${S}|${P}|`;
      for (const X of LIM_E) this.cell(base + X).n++;
      let fi = -1;
      for (let i = 0; i < n; i++) {
        if (t[i] > S || dur - t[i] < 3) break;
        const low = v === "s5" ? bid[i] : mn[i];
        if (low !== null && low <= P / 100 + 1e-9) { fi = i; break; }
      }
      if (fi >= 0) this.finish(base, v, fi, P / 100, t, bid, mx, dur, won, day);
    }
  }
  out(): Record<string, unknown> {
    const days = [...this.days].sort();
    const cells: Record<string, (number | number[])[]> = {};
    const qset = new Set<number>();
    for (const c of this.cells.values()) for (const b of c.q.keys()) qset.add(b);
    const qb = [...qset].sort((a, b) => a - b);
    for (const [k, c] of this.cells) {
      if (c.f === 0) continue;
      cells[k] = [c.n, c.f, c.h, c.w, Math.round(c.pnl * 1000) / 1000, Math.round(c.cost * 1000) / 1000, days.map((d) => Math.round((c.d.get(d) ?? 0) * 1000) / 1000), qb.map((b) => Math.round((c.q.get(b) ?? 0) * 1000) / 1000)];
    }
    const used = new Map<string, number>();
    const mk: unknown[] = [];
    const fl: Record<string, unknown> = {};
    for (const [k, f] of this.fills) {
      fl[k] = [f.cnt, f.pick.map((p) => {
        let i = used.get(p.mk);
        if (i === undefined) { i = mk.length; used.set(p.mk, i); const e = this.mkt.get(p.mk)!; mk.push([p.mk.slice(0, p.mk.lastIndexOf(".")), e.c, e.w, e.sd, e.win, e.o, e.t, e.b, e.lo, e.hi]); }
        return [i, p.ft, p.pay, p.ex];
      })];
    }
    return { qs: qb.map((b) => b * 14400), fills: fl, mk, L: LIM_L, X: LIM_X, W: LIM_W, E: LIM_E, DD: DYN_D, DU: DYN_U, DB: DYN_B, DW: DYN_W, CS: CRA_S, CP: CRA_P, days, markets: this.markets, wkMarkets: this.wkMarkets, cells };
  }
}

export async function buildPageData(dir: string, opt: PageOptions): Promise<PageData> {
  const coins: string[] = [];
  const ideas: string[] = [];
  const combos: string[] = [];
  const coinMap = new Map<string, number>();
  const ideaMap = new Map<string, number>();
  const comboMap = new Map<string, number>();
  const idx = (map: Map<string, number>, list: string[], v: string): number => {
    let i = map.get(v);
    if (i === undefined) {
      i = list.length;
      list.push(v);
      map.set(v, i);
    }
    return i;
  };

  const store = new ColStore(ID_COLS.length + COLS.length);
  const byCell = new Map<string, number[]>();
  const pathsAll = new Map<string, (number | null)[]>();
  const pmAll = new Map<string, [number, number, string, string]>();
  const cellAgg = new Map<string, { n: number; w: number; roi: number; px: number; mk: Set<number> }>();
  let mkId = 0;
  let markets = 0;
  let trades = 0;
  let firstTs = Infinity;
  let lastTs = 0;

  const limAgg = new LimAgg();
  let paperLim: unknown = null;
  try {
    const o = JSON.parse(await fs.promises.readFile(path.resolve(dir, "..", "lab-lim.json"), "utf8"));
    paperLim = { since: o.since ?? null, stats: o.stats ?? {}, log: Array.isArray(o.log) ? o.log : [], variants: limInfo() };
  } catch {
    paperLim = null; // бумажный тест ещё не запускался
  }
  for await (const m of readMarketRecords(dir, opt.sinceMs)) {
    if (opt.coin && m.coin !== opt.coin) continue;
    if (opt.wm && m.wm !== opt.wm) continue;
    markets++;
    try { limAgg.add(m); } catch { /* замер best-effort */ }
    const id = mkId++;
    const series: [Series | null, Series | null] = [null, null];
    const ci = idx(coinMap, coins, m.coin);
    {
      const minE: [number, number] = [Infinity, Infinity];
      for (const tr of m.trades) {
        if (tr.i.startsWith("temka:")) continue;
        const e = (tr.ts - m.open) / 1000;
        if (e < minE[tr.s]) minE[tr.s] = e;
      }
      for (const sd of [0, 1] as const) {
        if (minE[sd] < Infinity) {
          const pm = pathMk(m, sd, minE[sd], series);
          if (pm) pmAll.set(`${id}.${sd}`, pm);
        }
      }
    }
    for (const tr of m.trades) {
      if (tr.i.startsWith("temka:")) continue; // v4: «темки» на странице не показываем (и не кладём в файл)
      const ii = idx(ideaMap, ideas, tr.i);
      const co = idx(comboMap, combos, tr.c);
      const row = computeRow(m, tr, id, ci, ii, co, series);
      const key = `${tr.i}|${tr.c}`;
      let list = byCell.get(key);
      if (!list) {
        list = [];
        byCell.set(key, list);
      }
      list.push(store.push(row));
      const won = tr.s === m.win ? 1 : 0;
      const roi = won ? (1 - tr.px) / tr.px : -1;
      let a = cellAgg.get(key);
      if (!a) {
        a = { n: 0, w: 0, roi: 0, px: 0, mk: new Set() };
        cellAgg.set(key, a);
      }
      a.n++;
      a.w += won;
      a.roi += roi;
      a.px += tr.px;
      a.mk.add(id);
      trades++;
      if (tr.ts < firstTs) firstTs = tr.ts;
      if (tr.ts > lastTs) lastTs = tr.ts;
    }
  }

  // потолок строк: равномерная выборка внутри каждой ячейки (детерминированная)
  const sel: number[] = [];
  let sampled = false;
  const rnd01 = mulberry(12345);
  if (trades <= opt.maxRows) {
    for (const list of byCell.values()) for (const r of list) sel.push(r);
  } else {
    // ячейки с малым числом сделок берём целиком, остаток бюджета делим между большими
    const entries = [...byCell.entries()].sort((a, b) => a[1].length - b[1].length);
    let budget = opt.maxRows;
    let left = entries.length;
    for (const [, list] of entries) {
      const share = Math.max(40, Math.floor(budget / left));
      if (list.length <= share) {
        for (const r of list) sel.push(r);
        budget -= list.length;
      } else {
        const picked = list.slice();
        for (let i = picked.length - 1; i > 0; i--) {
          const j = Math.floor(rnd01() * (i + 1));
          [picked[i], picked[j]] = [picked[j], picked[i]];
        }
        for (let i = 0; i < share; i++) sel.push(picked[i]);
        budget -= share;
        sampled = true;
      }
      left--;
    }
  }
  const paths: Record<string, (number | null)[]> = {};
  void pathsAll;
  const pm: Record<string, [number, number, string, string]> = {};
  for (const i of sel) {
    const k = `${store.get(7, i)}.${store.get(2, i)}`;
    const p = pmAll.get(k);
    if (p) pm[k] = p;
  }

  // рынки без сделок (только лента) — нужны только для сетки лимиток
  try {
    for await (const m of readMarketRecords(path.resolve(dir, "..", "lab-tape"), opt.sinceMs)) {
      if (opt.coin && m.coin !== opt.coin) continue;
      if (opt.wm && m.wm !== opt.wm) continue;
      limAgg.add(m);
    }
  } catch { /* папки может не быть */ }
  const cells: Record<string, number[]> = {};
  for (const [k, a] of cellAgg) cells[k] = [a.n, a.w, a.roi, a.px, a.mk.size];

  return {
    meta: {
      generatedAt: Date.now(),
      periodLabel: opt.periodLabel,
      coin: opt.coin ?? null,
      wm: opt.wm ?? null,
      markets,
      trades,
      rowsEmbedded: sel.length,
      sampled,
      firstTs: Number.isFinite(firstTs) ? firstTs : null,
      lastTs: lastTs || null,
    },
    coins,
    ideas,
    combos,
    cols: COLS,
    exchanges: [{ label: "Binance", b: "bk", r: "bin" }, { label: "Coinbase", b: "ck", r: "cbr" }, ...XEX.map((x) => ({ label: x.label, b: `xb_${x.key}`, r: `xr_${x.key}` }))],
    idCols: ID_COLS,
    offsets: OFFSETS,
    rows: [],
    store,
    sel,
    paths,
    pm,
    cells,
    lim: limAgg.out(),
    paperLim: paperLim,
  };
}

/** Выбранные строки как обычные массивы (нужно только для небольших сводок). */
export function materializeRows(data: PageData): (number | null)[][] {
  if (!data.store) return data.rows;
  const st = data.store;
  return (data.sel ?? []).map((i) => st.row(i).map((v, j) => (v === null ? null : COL_DEC[j] === 0 ? Math.round(v) : Math.round(v * Math.pow(10, COL_DEC[j])) / Math.pow(10, COL_DEC[j]))));
}

/** Собирает HTML: данные кладутся в <script type="application/json">. Сжатие потоковое — столбец за столбцом (мало памяти). */
export async function renderPageHtml(data: PageData): Promise<string> {
  // v3.8: сжимаем строки сделок (gzip+base64) — в файл помещаются все сделки, страница распаковывает их при открытии
  const { rows, paths, pm, store: _s, sel: _l, ...rest } = data;
  void _s;
  void _l;
  const json = JSON.stringify({ ...rest, rows: [] }).replace(/</g, "\\u003c");
  const gz = zlib.createGzip({ level: 9 });
  const chunks: Buffer[] = [];
  gz.on("data", (c: Buffer) => chunks.push(c));
  const done = new Promise<void>((res, rej) => {
    gz.on("end", () => res());
    gz.on("error", rej);
  });
  const w = (str: string): Promise<void> =>
    new Promise<void>((res) => {
      if (gz.write(str)) res();
      else gz.once("drain", () => res());
    });
  const useStore = data.store !== undefined;
  const sel = data.sel ?? [];
  const nr = useStore ? sel.length : rows.length;
  let nc = 0;
  if (useStore) nc = data.store!.nc;
  else for (const r of rows) if (r.length > nc) nc = r.length;
  await w(`{"n":${nr},"c":[`);
  for (let j = 0; j < nc; j++) {
    const c: (number | null)[] = new Array(nr);
    if (useStore) {
      const st = data.store!;
      const arr = st.cols[j];
      const d = COL_DEC[j] ?? 3;
      const f = Math.pow(10, d);
      for (let i = 0; i < nr; i++) {
        const v = arr[sel[i]];
        c[i] = v !== v ? null : d === 0 ? Math.round(v) : Math.round(v * f) / f;
      }
    } else {
      for (let i = 0; i < nr; i++) {
        const v = rows[i][j];
        c[i] = v === undefined ? null : v;
      }
    }
    await w((j ? "," : "") + JSON.stringify(c));
  }
  await w(`],"p":${JSON.stringify(paths ?? {})},"pm":${JSON.stringify(pm ?? {})}}`);
  gz.end();
  await done;
  const b64 = Buffer.concat(chunks).toString("base64");
  chunks.length = 0;
  return PAGE_TEMPLATE.replace("/*__ROWS__*/", () => b64).replace("/*__DATA__*/null", () => json);
}

/** Оставляет пути только тех рынков/сторон, на которые ссылаются оставшиеся строки. */
function prunePm(data: PageData): void {
  if (!data.pm || !data.sel || !data.store) return;
  const st = data.store;
  const keep = new Set(data.sel.map((i) => `${st.get(7, i)}.${st.get(2, i)}`));
  for (const k of Object.keys(data.pm)) if (!keep.has(k)) delete data.pm[k];
}

/** Ужимает уже собранные данные до maxBytes (без повторного чтения диска): оставляет случайную долю строк. */
export async function shrinkPage(data: PageData, maxBytes: number): Promise<string> {
  let html = await renderPageHtml(data);
  // v4.9: сначала убираем целиком новые «монетные» идеи (cdx/cbk/cac/cpb/cbd/crv) — они появились недавно и раздували файл; старые данные остаются как были
  if (Buffer.byteLength(html) > maxBytes && data.sel && data.store) {
    const expIdx = new Set<number>();
    data.ideas.forEach((n, i) => {
      if (/^c(dx|bk|ac|pb|bd|rv)$/.test(n)) expIdx.add(i);
    });
    if (expIdx.size) {
      const st = data.store;
      const before = data.sel.length;
      data.sel = data.sel.filter((i) => !expIdx.has(st.get(4, i) as number));
      if (data.sel.length < before) {
        data.meta = { ...data.meta, rowsEmbedded: data.sel.length, droppedExp: before - data.sel.length };
        prunePm(data);
        html = await renderPageHtml(data);
      }
    }
  }
  for (let attempt = 0; attempt < 5; attempt++) {
    const size = Buffer.byteLength(html);
    if (size <= maxBytes || !data.sel || data.sel.length <= 5000) break;
    const ratio = Math.max(0.05, Math.min(0.9, (maxBytes / size) * 0.9));
    const rnd01 = mulberry(777 + attempt);
    data.sel = data.sel.filter(() => rnd01() < ratio);
    data.meta = { ...data.meta, rowsEmbedded: data.sel.length, sampled: true };
    prunePm(data);
    html = await renderPageHtml(data);
  }
  return html;
}

/** Строит страницу и возвращает HTML. Если получилось больше maxBytes, оставляет только часть строк. */
export async function buildPage(
  dir: string,
  opt: Omit<PageOptions, "maxRows"> & { maxRows?: number },
  maxBytes = 45 * 1024 * 1024, // лимит Telegram на файл 50 МБ (с запасом на служебные байты)
): Promise<{ html: string; data: PageData }> {
  const data = await buildPageData(dir, { ...opt, maxRows: opt.maxRows ?? 5_000_000 });
  const html = await shrinkPage(data, maxBytes);
  return { html, data };
}

// ───────────────────────── Сводка по биржам (для Telegram) ─────────────────────────

/** Текст для «лаб биржи все»: по каждой бирже корзины 0-4 и совместные сигналы бирж, для профилей бота и «Модели цены». */
export async function buildExchangeSummary(dir: string, sinceMs: number, periodLabel: string): Promise<string> {
  const data = await buildPageData(dir, { sinceMs, periodLabel, maxRows: 2_000_000 });
  const allRows = materializeRows(data);
  const ideaIdx = (id: string): number => data.ideas.indexOf(id);
  const NI = ID_COLS.length;
  const col = (k: string): number => NI + COLS.findIndex((c) => c.k === k);
  const botSet = new Set(data.ideas.map((n, i) => (/^bot\d+$/.test(n) ? i : -1)).filter((i) => i >= 0));
  const modelI = ideaIdx("model");
  const groups: { title: string; test: (r: (number | null)[]) => boolean }[] = [
    { title: "Профиль 7 (как в «лаб бинанс»)", test: (r) => r[4] === ideaIdx("bot7") },
    { title: "Профили бота 1-7 вместе", test: (r) => botSet.has(r[4] as number) },
    { title: "Модель цены (вся сетка)", test: (r) => modelI >= 0 && r[4] === modelI },
  ];
  const pxI = col("px");
  const st = (rs: (number | null)[][]): string => {
    if (rs.length === 0) return "нет данных";
    let w = 0, roi = 0, px = 0;
    for (const r of rs) {
      const won = r[3] === 1;
      const p = r[pxI] as number;
      w += won ? 1 : 0;
      px += p;
      roi += won ? (1 - p) / p : -1;
    }
    const n = rs.length;
    const edge = (w / n - px / n) * 100;
    return `${n} сд., побед ${((w / n) * 100).toFixed(1)}% (цена ${(px / n).toFixed(2)}, запас ${edge >= 0 ? "+" : ""}${edge.toFixed(1)} п.п.), ROI ${((roi / n) * 100).toFixed(1)}%`;
  };
  const BL = ["против", "меньше 50%", "50-80%", "80-120%", "больше 120%"];
  const lines: string[] = [];
  lines.push(`<b>🏦 Биржи: каждая отдельно и вместе (${periodLabel})</b>`);
  lines.push(`Рынков ${data.meta.markets}, сделок ${data.meta.trades}. Данные по новым биржам копятся с момента их подключения.`);
  lines.push("Корзина = во сколько раз биржа прошла от открытия окна относительно потока бота: «против» — биржа уже по другую сторону.");
  lines.push("Сделки одного рынка в разных ячейках пересекаются, поэтому смотри на порядок, а не на сотые доли.");
  for (const g of groups) {
    const rows = allRows.filter(g.test);
    lines.push("");
    lines.push(`<b>━━ ${g.title}: ${rows.length} сд. ━━</b>`);
    for (const ex of data.exchanges) {
      const bi = col(ex.b);
      const have = rows.filter((r) => r[bi] !== null);
      lines.push(`<b>${ex.label}</b> (данные есть у ${have.length})`);
      if (have.length === 0) {
        lines.push("  пока нет данных");
        continue;
      }
      for (let b = 0; b < 5; b++) lines.push(`  ${BL[b]}: ${st(have.filter((r) => r[bi] === b))}`);
    }
    const iNag = col("nag"), iNcf = col("ncf"), iNex = col("nex");
    const haveC = rows.filter((r) => r[iNex] !== null && (r[iNex] as number) >= 2);
    lines.push("<b>Вместе: сколько бирж против</b>");
    for (const [name, f] of [
      ["ни одна", (n: number) => n === 0],
      ["1", (n: number) => n === 1],
      ["2", (n: number) => n === 2],
      ["3 и больше", (n: number) => n >= 3],
    ] as [string, (n: number) => boolean][]) lines.push(`  ${name}: ${st(haveC.filter((r) => f(r[iNag] as number)))}`);
    lines.push("<b>Вместе: сколько бирж подтверждают (≥50% потока)</b>");
    for (const [name, f] of [
      ["0", (n: number) => n === 0],
      ["1", (n: number) => n === 1],
      ["2", (n: number) => n === 2],
      ["3", (n: number) => n === 3],
      ["4 и больше", (n: number) => n >= 4],
    ] as [string, (n: number) => boolean][]) lines.push(`  ${name}: ${st(haveC.filter((r) => f(r[iNcf] as number)))}`);
    lines.push(`  ВСЕ биржи с данными подтверждают: ${st(haveC.filter((r) => r[iNcf] === r[iNex]))}`);
  }
  lines.push("");
  lines.push("Любую комбинацию бирж (например OKX против + Binance подтверждает) и разбор по ячейкам — на странице: «лаб страница».");
  return lines.join("\n");
}
