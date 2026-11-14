/**
 * ЛАБ ЛИМИТКИ v1 — «бумажная» проверка идей про прострелы по ЖИВОМУ стакану (ничего не покупает и не продаёт).
 * Зачем: на странице «🪤 лимитки» расчёт идёт по ленте (bid раз в 5с) и может показывать покупки, которых в реальности не было
 * (дырки в стакане в первые секунды рынка). Здесь вход засчитывается ТОЛЬКО если живой ask уже опустился до нашей цены
 * и в стакане на этой цене реально есть 5 акций (цена входа = средневзвешенная по глубине, как у настоящего ордера FAK).
 * Выход так же: продажа по реальной глубине bid. Итог рынка — по resolve.
 * Три идеи: «обвал на старте» (C…), «динамическая лимитка» (D…), «лимитка на одной цене» (S…).
 * Свои данные (lab-lim.json), своя команда «лаб лимитки».
 */
import * as fs from "fs";
import * as path from "path";

export interface LimMarket {
  eventSlug: string;
  coin: string;
  windowMinutes: number;
  closeTimeMs: number;
  upTokenId: string;
  downTokenId: string;
}
type Side = "Up" | "Down";
type Exit = { k: "hold" } | { k: "tp"; c: number } | { k: "t"; s: number };
type Kind = { k: "cr"; S: number; P: number } | { k: "dy"; D: number } | { k: "st"; L: number };

interface Variant { id: string; kind: Kind; exit: Exit; }
export interface LimDeps {
  fetchBook: (tokenId: string) => Promise<{ bids: Array<[number, number]>; asks: Array<[number, number]> } | null>;
  dataDir: string;
}

const SHARES = 5;
const MAX_PRICE_SLIP = 0.03; // если по глубине средняя цена входа хуже сигнала больше чем на 3¢ — вход не засчитываем
const MIN_LEFT_SEC = 10;
const EXITS: Exit[] = [{ k: "hold" }, { k: "tp", c: 5 }, { k: "t", s: 10 }];

function buildVariants(): Variant[] {
  const out: Variant[] = [];
  let c = 0, d = 0, s = 0;
  const id = (p: string, n: number) => p + String(n).padStart(2, "0");
  for (const S of [15, 60]) for (const P of [10, 20]) for (const e of EXITS) out.push({ id: id("C", ++c), kind: { k: "cr", S, P }, exit: e });
  for (const D of [10, 20, 30]) for (const e of EXITS) out.push({ id: id("D", ++d), kind: { k: "dy", D }, exit: e });
  for (const L of [30, 45]) for (const e of [EXITS[0], EXITS[1]]) out.push({ id: id("S", ++s), kind: { k: "st", L }, exit: e });
  return out;
}
export const LIM_VARIANTS = buildVariants();

const exitTxt = (e: Exit): string => (e.k === "hold" ? "держим до резолва" : e.k === "tp" ? `продаём на +${e.c}¢` : `продаём по рынку через ${e.s}с`);
const kindTxt = (k: Kind): string =>
  k.k === "cr" ? `обвал на старте: в первые ${k.S}с ask ≤ ${k.P}¢`
  : k.k === "dy" ? `динамическая лимитка на ${k.D}% ниже цены (подтягивается каждые 5с)`
  : `лимитка ${k.L}¢ (ставим с 10-й секунды, если ask выше)`;
export const limInfo = (): Array<{ id: string; rule: string }> => LIM_VARIANTS.map((v) => ({ id: v.id, rule: limRuleTxt(v) }));
export const limRuleTxt = (v: Variant): string => `${kindTxt(v.kind)} · ${exitTxt(v.exit)}`;

interface Pos {
  v: Variant;
  slug: string;
  coin: string;
  side: Side;
  tokenId: string;
  openMs: number;
  closeMs: number;
  phase: "wait" | "filling" | "open" | "exiting" | "done";
  ref: number | null;
  refTs: number;
  lim: number | null;
  placed: boolean;
  noDepthMarked: boolean;
  nextTry: number;
  tries: number;
  fillMs: number;
  fillSec: number;
  entry: number;
  trigAsk: number;
}
interface VStat {
  trig: number; // сколько позиций дошли до сигнала «ask опустился до цены»
  noDepth: number; // из них — в стакане не было 5 акций по такой цене (мнимые входы)
  fills: number;
  exits: number; // выход сработал до резолва
  settled: number; // дошли до резолва
  wins: number; // токен выиграл (среди дошедших до резолва)
  pnl: number;
  cost: number;
  bad: number; // сделок в минус
}
interface LogRec { t: number; v: string; slug: string; coin: string; side: Side; sec: number; trig: number; entry: number; how: string; exit: number | null; pnl: number; won: boolean | null }
const newStat = (): VStat => ({ trig: 0, noDepth: 0, fills: 0, exits: 0, settled: 0, wins: 0, pnl: 0, cost: 0, bad: 0 });
const r2 = (x: number): number => Math.round(x * 100) / 100;
const money = (x: number): string => (x >= 0 ? "+" : "−") + "$" + Math.abs(x).toFixed(2);

export class LabLim {
  private pos = new Map<string, Pos>();
  private stats: Record<string, VStat> = {};
  private log: LogRec[] = [];
  private file: string;
  private dirty = false;
  private since = Date.now();

  constructor(private deps: LimDeps) {
    this.file = path.resolve(deps.dataDir, "lab-lim.json");
    try {
      const o = JSON.parse(fs.readFileSync(this.file, "utf8"));
      this.stats = o.stats ?? {};
      this.log = Array.isArray(o.log) ? o.log : [];
      if (typeof o.since === "number") this.since = o.since;
    } catch { /* файла ещё нет */ }
    setInterval(() => this.save(), 60_000).unref?.();
    setInterval(() => this.timer(), 1000).unref?.();
  }
  private save(): void {
    if (!this.dirty) return;
    this.dirty = false;
    try { fs.writeFileSync(this.file, JSON.stringify({ since: this.since, stats: this.stats, log: this.log.slice(-8000) })); } catch { /* не критично */ }
  }
  private st(id: string): VStat { return (this.stats[id] ??= newStat()); }
  reset(): void { this.pos.clear(); this.stats = {}; this.log = []; this.since = Date.now(); this.dirty = true; this.save(); }

  /** Каждое обновление цены токена. */
  onTick(m: LimMarket, side: Side, bid: number | null, ask: number | null, now: number): void {
    const openMs = m.closeTimeMs - m.windowMinutes * 60000;
    const sec = (now - openMs) / 1000;
    const left = (m.closeTimeMs - now) / 1000;
    if (sec < 0 || left < MIN_LEFT_SEC) return;
    const tokenId = side === "Up" ? m.upTokenId : m.downTokenId;
    for (const v of LIM_VARIANTS) {
      const key = `${m.eventSlug}|${side}|${v.id}`;
      let p = this.pos.get(key);
      if (!p) {
        if (v.kind.k === "cr" && sec > v.kind.S) continue;
        p = { v, slug: m.eventSlug, coin: m.coin, side, tokenId, openMs, closeMs: m.closeTimeMs, phase: "wait", ref: null, refTs: 0, lim: null, placed: false, noDepthMarked: false, nextTry: 0, tries: 0, fillMs: 0, fillSec: 0, entry: 0, trigAsk: 0 };
        this.pos.set(key, p);
      }
      if (p.phase === "wait") this.tickWait(p, bid, ask, sec, now);
      else if (p.phase === "open") this.tickOpen(p, bid, now);
    }
  }

  private tickWait(p: Pos, bid: number | null, ask: number | null, sec: number, now: number): void {
    const k = p.v.kind;
    let price: number | null = null; // цена, на которой хотим купить
    if (k.k === "cr") {
      if (sec > k.S) { p.phase = "done"; return; }
      price = k.P / 100;
    } else if (k.k === "dy") {
      if (p.lim !== null && p.lim >= 0.02) price = p.lim;
      if (bid !== null && bid > 0 && now - p.refTs >= 5000) { p.ref = bid; p.lim = bid * (1 - k.D / 100); p.refTs = now; if (price === null) return; }
    } else {
      if (!p.placed) {
        if (sec < 10) return;
        if (ask !== null && ask > k.L / 100) p.placed = true; else { p.phase = "done"; return; }
      }
      price = k.L / 100;
    }
    if (price === null || ask === null || ask <= 0 || ask > price + 1e-9) return;
    if (now < p.nextTry) return;
    p.phase = "filling";
    this.st(p.v.id); // создать
    void this.tryFill(p, price, ask, sec, now);
  }

  private async tryFill(p: Pos, price: number, ask: number, sec: number, now: number): Promise<void> {
    const s = this.st(p.v.id);
    const book = await this.deps.fetchBook(p.tokenId);
    // vwap покупки 5 акций по реальной глубине
    let need = SHARES, cost = 0;
    if (book) for (const [px, sz] of book.asks) { const take = Math.min(need, sz); cost += take * px; need -= take; if (need <= 1e-9) break; }
    const ok = !!book && need <= 1e-9 && cost / SHARES <= price + MAX_PRICE_SLIP + 1e-9;
    if (!p.noDepthMarked && p.tries === 0) s.trig++;
    p.tries++;
    if (!ok) {
      if (!p.noDepthMarked) { p.noDepthMarked = true; s.noDepth++; }
      p.phase = "wait";
      p.nextTry = Date.now() + 3000;
      this.dirty = true;
      return;
    }
    if (p.noDepthMarked) s.noDepth--; // потом всё-таки нашлась глубина — это не мнимый вход
    p.noDepthMarked = false;
    p.entry = cost / SHARES;
    p.trigAsk = ask;
    p.fillMs = Date.now();
    p.fillSec = Math.round((p.fillMs - p.openMs) / 1000);
    p.phase = "open";
    s.fills++;
    s.cost += cost;
    this.dirty = true;
    void now; void sec;
  }

  private tickOpen(p: Pos, bid: number | null, now: number): void {
    const e = p.v.exit;
    if (e.k === "tp" && bid !== null && bid >= p.entry + e.c / 100 - 1e-9 && now >= p.nextTry) {
      p.phase = "exiting";
      void this.tryExit(p, p.entry + e.c / 100, "лимитка");
    }
  }

  private timer(): void {
    const now = Date.now();
    for (const [key, p] of this.pos) {
      if (p.phase === "done" && now > p.closeMs + 6 * 60000) { this.pos.delete(key); continue; }
      if (p.phase !== "open") continue;
      const e = p.v.exit;
      if (e.k === "t" && now - p.fillMs >= e.s * 1000 && now >= p.nextTry && (p.closeMs - now) / 1000 > 5) {
        p.phase = "exiting";
        void this.tryExit(p, 0, "по рынку");
      }
    }
  }

  private async tryExit(p: Pos, minPrice: number, how: string): Promise<void> {
    const book = await this.deps.fetchBook(p.tokenId);
    let need = SHARES, got = 0;
    if (book) for (const [px, sz] of book.bids) { const take = Math.min(need, sz); got += take * px; need -= take; if (need <= 1e-9) break; }
    const vwap = got / SHARES;
    const ok = !!book && need <= 1e-9 && vwap >= minPrice - 0.02 - 1e-9;
    if (!ok) {
      p.tries++;
      p.nextTry = Date.now() + 2000;
      p.phase = p.tries > 12 ? "open" : "open"; // остаёмся в позиции, повторим; если не выйдем — доиграет резолв
      return;
    }
    const s = this.st(p.v.id);
    const pnl = got - p.entry * SHARES;
    s.exits++;
    s.pnl += pnl;
    if (pnl < 0) s.bad++;
    if (this.log.length > 9000) this.log.splice(0, 1000);
    this.log.push({ t: Date.now(), v: p.v.id, slug: p.slug, coin: p.coin, side: p.side, sec: p.fillSec, trig: p.trigAsk, entry: r2(p.entry * 100) / 100, how, exit: r2(vwap * 100) / 100, pnl: r2(pnl), won: null });
    p.phase = "done";
    this.dirty = true;
  }

  /** Итог рынка: позиции, дошедшие до резолва. */
  onSettle(slug: string, winner: Side): void {
    for (const [key, p] of [...this.pos]) {
      if (p.slug !== slug) continue;
      if (p.phase === "open" || p.phase === "exiting" || p.phase === "filling") {
        if (p.phase === "filling") { this.pos.delete(key); continue; } // вход не успел подтвердиться — не считаем
        const s = this.st(p.v.id);
        const won = p.side === winner;
        const pnl = (won ? SHARES : 0) - p.entry * SHARES;
        s.settled++;
        if (won) s.wins++;
        s.pnl += pnl;
        if (pnl < 0) s.bad++;
        if (this.log.length > 9000) this.log.splice(0, 1000);
    this.log.push({ t: Date.now(), v: p.v.id, slug: p.slug, coin: p.coin, side: p.side, sec: p.fillSec, trig: p.trigAsk, entry: r2(p.entry * 100) / 100, how: "до резолва", exit: won ? 1 : 0, pnl: r2(pnl), won });
        this.dirty = true;
      }
      this.pos.delete(key);
    }
  }

  statusText(arg?: string): string {
    const days = ((Date.now() - this.since) / 86400000).toFixed(1);
    const a = (arg ?? "").trim().toUpperCase();
    if (a) {
      const v = LIM_VARIANTS.find((x) => x.id === a);
      if (!v) return `Нет варианта «${a}». Список: «лаб лимитки».`;
      const s = this.st(v.id);
      const L: string[] = [`🪤 ${v.id}: ${limRuleTxt(v)}`, `Идёт ${days} дн. · 5 акций на сделку, вход только по живому ask с реальной глубиной`];
      L.push(`Сигналов «ask дошёл до цены»: ${s.trig}; из них в стакане не было 5 акций по такой цене: ${s.noDepth}; входов засчитано: ${s.fills}`);
      L.push(`Вышли до резолва: ${s.exits}; дошли до резолва: ${s.settled}, из них токен выиграл ${s.wins}${s.settled ? ` (${Math.round((s.wins / s.settled) * 100)}%)` : ""}`);
      L.push(`Итог: ${money(s.pnl)} на вложенные ~$${s.cost.toFixed(0)}${s.cost > 0 ? ` (ROI ${((s.pnl / s.cost) * 100).toFixed(1)}%)` : ""}, сделок в минус: ${s.bad}`);
      const rows = this.log.filter((x) => x.v === v.id).slice(-12).reverse();
      if (rows.length) {
        L.push("", "Последние сделки (вход → выход, на какой секунде рынка купили):");
        for (const r of rows) L.push(`${new Date(r.t + 7 * 3600e3).toISOString().slice(5, 16).replace("T", " ")} ${r.slug} ${r.side}: купили ${r.sec}с по ${r.entry.toFixed(2)} (ask на сигнале ${r.trig.toFixed(2)}) → ${r.how}${r.exit !== null ? " " + r.exit.toFixed(2) : ""} · ${money(r.pnl)}${r.won === null ? "" : r.won ? " · токен выиграл" : " · токен проиграл"}`);
      }
      return L.join("\n");
    }
    const L: string[] = [`🪤 ЛАБ ЛИМИТКИ (бумажные, по живому стакану) — идёт ${days} дн.`, "вариант · входов (мнимых без глубины) · токен выиграл на резолве · итог"];
    let tot = 0;
    for (const v of LIM_VARIANTS) {
      const s = this.st(v.id);
      tot += s.pnl;
      L.push(`${v.id} ${limRuleTxt(v)}: входов ${s.fills} (без глубины ${s.noDepth}) · ${s.settled ? `${s.wins}/${s.settled} на резолве` : "—"} · ${money(s.pnl)}`);
    }
    L.push("", `Сумма по всем: ${money(tot)}. Подробно: «лаб лимитки C01». Сброс: «лаб лимитки сброс».`);
    return L.join("\n");
  }
}
