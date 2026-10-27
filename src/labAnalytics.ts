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
  fe?: number | null;
  xings?: number | null;
  agreeN?: number | null;
  c05?: number | null;
  c10?: number | null;
  hr?: number | null;
  pk?: number | null; // пик цены токена до входа
  bsz?: number | null;
  asz?: number | null;
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
const TP_DELTAS_PAGE = [0.03, 0.05, 0.1];
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
  ...TP_DELTAS_PAGE.map((d) => ({ k: `tp${Math.round(d * 100)}`, label: `Секунд до bid ≥ вход+${d.toFixed(2)}`, g: "tp" as const, d: 0, hint: "пусто — цель не достигнута" })),
];

const COL_IDX: Record<string, number> = {};
COLS.forEach((c, i) => (COL_IDX[c.k] = ID_COLS.length + i));

// ───────────────────────── Расчёт показателей сделки ─────────────────────────

function rnd(x: number | null | undefined, d: number): number | null {
  if (x === null || x === undefined || !Number.isFinite(x)) return null;
  const f = Math.pow(10, d);
  return Math.round(x * f) / f;
}

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
  set("fe", tr.fe, 3);
  set("xings", tr.xings, 0);
  set("agree", tr.agreeN, 0);
  set("c05", tr.c05, 0);
  set("c10", tr.c10, 0);
  set("hr", tr.hr, 0);
  set("pk", tr.pk, 3);
  set("bsz", tr.bsz, 0);
  set("asz", tr.asz, 0);

  // до входа (по ленте)
  const bidNow = tr.bid0 ?? valAt(se.t, se.bid, entryT);
  if (bidNow !== null && bidNow !== undefined) {
    const b30 = valAt(se.t, se.bid, entryT - 30);
    const b60 = valAt(se.t, se.bid, entryT - 60);
    if (b30 !== null && entryT - 30 >= -1) set("tb30", bidNow - b30, 3);
    if (b60 !== null && entryT - 60 >= -1) set("tb60", bidNow - b60, 3);
  }
  const c0 = pmBps ?? valAt(se.t, se.coin, entryT);
  if (c0 !== null && c0 !== undefined && entryT - 30 >= -1) {
    const cPrev = valAt(se.t, se.coin, entryT - 30);
    if (cPrev !== null) set("cmp30", c0 - cPrev, 1);
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
  let first10: number | null = null;
  let fell50 = false;
  let seen = 0;
  const tpT: (number | null)[] = TP_DELTAS_PAGE.map(() => null);
  for (let i = 0; i < se.t.length; i++) {
    if (se.t[i] <= entryT) continue;
    const b = se.bid[i];
    if (b === null || b === undefined) continue;
    seen++;
    const dlt = b - px;
    if (dlt > mx) mx = dlt;
    if (dlt < mn) {
      mn = dlt;
      tmn = se.t[i] - entryT;
    }
    if (first10 === null && dlt <= -0.1 + 1e-9) first10 = se.t[i] - entryT;
    if (b < 0.5) fell50 = true;
    TP_DELTAS_PAGE.forEach((d, j) => {
      if (tpT[j] === null && dlt >= d - 1e-9) tpT[j] = se.t[i] - entryT;
    });
  }
  if (seen > 0) {
    set("mfe", mx, 3);
    set("mae", mn, 3);
    set("tmae", tmn, 0);
    set("lost10", first10 !== null ? 1 : 0, 0);
    if (px >= 0.55) set("flip", fell50 ? 1 : 0, 0);
  }
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

export interface PageData {
  meta: Record<string, unknown>;
  coins: string[];
  ideas: string[];
  combos: string[];
  cols: ColDef[];
  idCols: string[];
  offsets: number[];
  rows: (number | null)[][];
  exchanges: { label: string; b: string; r: string }[];
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

  const byCell = new Map<string, (number | null)[][]>();
  const cellAgg = new Map<string, { n: number; w: number; roi: number; px: number; mk: Set<number> }>();
  let mkId = 0;
  let markets = 0;
  let trades = 0;
  let firstTs = Infinity;
  let lastTs = 0;

  for await (const m of readMarketRecords(dir, opt.sinceMs)) {
    if (opt.coin && m.coin !== opt.coin) continue;
    if (opt.wm && m.wm !== opt.wm) continue;
    markets++;
    const id = mkId++;
    const series: [Series | null, Series | null] = [null, null];
    const ci = idx(coinMap, coins, m.coin);
    for (const tr of m.trades) {
      const ii = idx(ideaMap, ideas, tr.i);
      const co = idx(comboMap, combos, tr.c);
      const row = computeRow(m, tr, id, ci, ii, co, series);
      const key = `${tr.i}|${tr.c}`;
      let list = byCell.get(key);
      if (!list) {
        list = [];
        byCell.set(key, list);
      }
      list.push(row);
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
  const rows: (number | null)[][] = [];
  const nCells = Math.max(1, byCell.size);
  const capPerCell = Math.max(40, Math.floor(opt.maxRows / nCells));
  let sampled = false;
  const rnd01 = mulberry(12345);
  // сначала считаем, сколько всего строк, и если помещаемся — берём всё
  if (trades <= opt.maxRows) {
    for (const list of byCell.values()) rows.push(...list);
  } else {
    // ячейки с малым числом сделок берём целиком, остаток бюджета делим между большими
    const entries = [...byCell.entries()].sort((a, b) => a[1].length - b[1].length);
    let budget = opt.maxRows;
    let left = entries.length;
    for (const [, list] of entries) {
      const share = Math.max(40, Math.floor(budget / left));
      if (list.length <= share) {
        rows.push(...list);
        budget -= list.length;
      } else {
        // выборка без повторов
        const picked = list.slice();
        for (let i = picked.length - 1; i > 0; i--) {
          const j = Math.floor(rnd01() * (i + 1));
          [picked[i], picked[j]] = [picked[j], picked[i]];
        }
        rows.push(...picked.slice(0, share));
        budget -= share;
        sampled = true;
      }
      left--;
    }
  }
  void capPerCell;

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
      rowsEmbedded: rows.length,
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
    rows,
    cells,
  };
}

/** Собирает HTML: данные кладутся в <script type="application/json">. */
export function renderPageHtml(data: PageData): string {
  const json = JSON.stringify(data).replace(/</g, "\\u003c");
  return PAGE_TEMPLATE.replace("/*__DATA__*/null", () => json);
}

/** Строит страницу и возвращает HTML. Если получилось больше maxBytes, пересобирает с меньшим числом строк. */
export async function buildPage(
  dir: string,
  opt: Omit<PageOptions, "maxRows"> & { maxRows?: number },
  maxBytes = 45 * 1024 * 1024,
): Promise<{ html: string; data: PageData }> {
  let maxRows = opt.maxRows ?? 100_000;
  for (let attempt = 0; attempt < 5; attempt++) {
    const data = await buildPageData(dir, { ...opt, maxRows });
    const html = renderPageHtml(data);
    if (Buffer.byteLength(html) <= maxBytes || maxRows <= 5000) return { html, data };
    maxRows = Math.floor(maxRows / 2);
  }
  const data = await buildPageData(dir, { ...opt, maxRows: 5000 });
  return { html: renderPageHtml(data), data };
}

// ───────────────────────── Сводка по биржам (для Telegram) ─────────────────────────

/** Текст для «лаб биржи все»: по каждой бирже корзины 0-4 и совместные сигналы бирж, для профилей бота и «Модели цены». */
export async function buildExchangeSummary(dir: string, sinceMs: number, periodLabel: string): Promise<string> {
  const data = await buildPageData(dir, { sinceMs, periodLabel, maxRows: 2_000_000 });
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
    const rows = data.rows.filter(g.test);
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
