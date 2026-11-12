/**
 * LAB TEST v1 — «бумажные» тесты разных ТС со стопами / входами после дна / тейками.
 * Ничего не покупает и не продаёт на бирже: на каждый сигнал нужной ячейки заводит виртуальную позицию (5 акций) для каждого варианта
 * из списка TEST_VARIANTS и по живым bid/ask симулирует вход, стоп, тейк и итог рынка.
 * Главное, что меряем: по какой цене стоп реально продался бы (глубина стакана в момент срабатывания) и насколько она хуже уровня стопа.
 * Не связан с торговым модулем и с основным списком «лаб тс»: свои данные (файл lab-test.json), своя команда «лаб тест».
 */
import * as fs from "fs";
import * as path from "path";

export interface TestMarket {
  eventSlug: string;
  coin: string;
  closeTimeMs: number;
  upTokenId: string;
  downTokenId: string;
}

type Side = "Up" | "Down";
type EntryRule = { k: "now" } | { k: "dip"; low: number; buy: number } | { k: "vyn"; peak: number; low: number; buy: number };
type StopRule = { u: "pct" | "c" | "abs" | "usd"; v: number; fast?: number };

export interface TestVariant {
  id: string;
  idea: string;
  combo: string;
  tsName: string; // как ТС называется на странице
  cfg?: string; // имя ТС из «лаб тс», чьё дополнительное условие (filter) тоже нужно (например cb4)
  entry: EntryRule;
  stop?: StopRule;
  take?: { c: number };
}

const TS_F2_080 = { idea: "fair2", combo: "edge <0 / 60-120с / 0.80-0.85", tsName: "Справедливая цена σ120с · edge <0 / 60-120с / 0.80-0.85" };
const TS_F2_060 = { idea: "fair2", combo: "edge <0 / 60-120с / 0.60-0.70", tsName: "Справедливая цена σ120с · edge <0 / 60-120с / 0.60-0.70" };
const TS_F1_070 = { idea: "fair1", combo: "edge <0 / 60-120с / 0.70-0.80", tsName: "Справедливая цена σ60с · edge <0 / 60-120с / 0.70-0.80" };
const TS_F1_6P = { idea: "fair1", combo: "edge 6%+ / 120-180с / 0.60-0.70", tsName: "Справедливая цена σ60с · edge 6%+ / 120-180с / 0.60-0.70" };
const TS_CB4 = { idea: "fair1", combo: "edge <0 / 120-180с / 0.60-0.70", tsName: "СБ4: σ60с · edge <0 / 120-180с / 0.60-0.70 + Coinbase/поток", cfg: "cb4" };
const imp = (a: string, b: string) => ({ idea: "impulse", combo: `0.03% за ${a}с / ${b}с`, tsName: `Импульс 0.03% за ${a}с / ${b}с` });

export const TEST_VARIANTS: TestVariant[] = [
  { id: "T01", ...TS_F2_080, entry: { k: "now" }, stop: { u: "c", v: 10, fast: 0.15 } },
  { id: "T02", ...TS_F2_080, entry: { k: "dip", low: 61, buy: 61 } },
  { id: "T03", ...TS_F2_080, entry: { k: "now" }, stop: { u: "pct", v: 2 } },
  { id: "T04", ...TS_F2_080, entry: { k: "now" }, stop: { u: "abs", v: 78 } },
  { id: "T05", ...TS_F2_080, entry: { k: "now" }, stop: { u: "abs", v: 79 } },
  { id: "T06", ...TS_F2_080, entry: { k: "now" }, stop: { u: "pct", v: 8 } },
  { id: "T07", ...TS_F2_080, entry: { k: "now" }, stop: { u: "pct", v: 4 } },
  { id: "T08", ...TS_F2_060, entry: { k: "now" }, stop: { u: "pct", v: 14 }, take: { c: 37 } },
  { id: "T09", ...TS_F1_070, entry: { k: "now" }, stop: { u: "pct", v: 18 } },
  { id: "T10", ...TS_F1_6P, entry: { k: "dip", low: 76, buy: 99 } },
  { id: "T11", ...TS_CB4, entry: { k: "now" }, stop: { u: "pct", v: 32, fast: 0.15 } },
  { id: "T12", ...imp("30", "180"), entry: { k: "now" }, stop: { u: "c", v: 10, fast: 0.15 } },
  { id: "T13", ...imp("30", "120"), entry: { k: "now" }, stop: { u: "usd", v: 0.5 } },
  { id: "T14", ...imp("15", "180"), entry: { k: "now" }, stop: { u: "usd", v: 0.75 } },
  { id: "T15", ...imp("15", "120"), entry: { k: "now" }, stop: { u: "usd", v: 0.5 } },
  { id: "T16", ...imp("30", "180"), entry: { k: "dip", low: 9, buy: 9 } },
  { id: "T17", ...imp("30", "120"), entry: { k: "dip", low: 10, buy: 10 } },
  { id: "T18", ...imp("30", "120"), entry: { k: "dip", low: 85, buy: 99 } },
  { id: "T19", ...imp("30", "180"), entry: { k: "vyn", peak: 50, low: 31, buy: 31 } },
];

const SHARES = 5;
const ASK_GAP = 3; // как «лаб дно спред 3»: покупаем, только если ask не выше уровня покупки + 3¢
const MIN_LEFT = 15; // как «лаб дно мин 15»: после этого входов по дну/выносу нет
const TRAIL_MS = 7000;
const LOG_MAX = 600;

const ruleTxt = (v: TestVariant): string => {
  const e = v.entry;
  const en = e.k === "now" ? "вход сразу" : e.k === "dip" ? `дно ≤${e.low}¢ → купить при ≥${e.buy}¢` : `рост до ${e.peak}¢ → вынос до ≤${e.low}¢ → купить при ≥${e.buy}¢`;
  let st = "";
  if (v.stop) {
    const s = v.stop;
    st = s.u === "pct" ? `стоп −${s.v}%` : s.u === "c" ? `стоп −${s.v}¢` : s.u === "abs" ? `стоп bid ≤${s.v}¢` : `стоп −$${s.v}`;
    if (s.fast) st += ` + только резкое падение ≥${s.fast}`;
  }
  const tk = v.take ? `, тейк +${v.take.c}¢` : "";
  return `${en}${st ? ", " + st : ", без стопа"}${tk}`;
};

interface Pos {
  vid: string;
  slug: string;
  coin: string;
  side: Side;
  tokenId: string;
  closeMs: number;
  sigTs: number;
  sigAsk: number;
  phase: "wait" | "open" | "closed" | "never";
  stage: number; // для дна/выноса: 0 ждём пик (только вынос), 1 ждём дно, 2 ждём возврат
  lowBid: number | null;
  buyTs?: number;
  entry?: number; // цена покупки (ask)
  buyBid?: number | null;
  level?: number; // уровень стопа (bid ≤ level)
  takeLevel?: number;
  trail: Array<[number, number]>;
  fs10?: number | null;
  fs20?: number | null;
  fsHeld?: boolean; // стоп отменён «только резким падением»: держим до конца
  minBid?: number;
  exit?: {
    kind: "stop" | "take";
    ts: number;
    bidHit: number;
    vwap?: number;
    depthOk?: boolean;
    bid2?: number | null;
    bid5?: number | null;
  };
}

interface VStat {
  signals: number; // сигналов ТС
  entered: number; // входов
  skipped: number; // сигнал был, но вход не случился (дно не пришло, поздно и т.п.)
  wins: number; // итог > 0
  pnl: number;
  stops: number;
  stopsFalse: number; // стоп сработал, а рынок потом выиграл нашей стороной
  takes: number;
  slipN: number;
  slipSum: number; // сумма (уровень стопа − реальная цена продажи), центов
  slipMax: number;
  depthFail: number;
  holdFs: number; // стоп не сработал из-за условия «резкое падение»
  pnlNoStop: number; // что было бы без стопа/тейка (держим до конца)
  entrySum: number;
}
const newStat = (): VStat => ({ signals: 0, entered: 0, skipped: 0, wins: 0, pnl: 0, stops: 0, stopsFalse: 0, takes: 0, slipN: 0, slipSum: 0, slipMax: 0, depthFail: 0, holdFs: 0, pnlNoStop: 0, entrySum: 0 });

interface LogRec {
  ts: number;
  vid: string;
  slug: string;
  coin: string;
  side: Side;
  entry: number;
  exit: string; // «стоп 0.62 → продали 0.55», «тейк …», «до конца»
  pnl: number;
  pnlHold: number;
  won: boolean;
}

export interface TestDeps {
  fetchBids: (tokenId: string) => Promise<Array<[number, number]> | null>;
  getBid: (tokenId: string) => number | null;
  dataDir: string;
  cfgFilter: (cfgName: string) => boolean; // прошёл ли сигнал дополнительное условие ТС из «лаб тс» (для СБ4)
}

export class LabTest {
  private pos = new Map<string, Pos>();
  private stats: Record<string, VStat> = {};
  private log: LogRec[] = [];
  private file: string;
  private dirty = false;
  private since = Date.now();

  constructor(private deps: TestDeps) {
    this.file = path.resolve(deps.dataDir, "lab-test.json");
    this.load();
    setInterval(() => this.save(), 60_000).unref?.();
  }

  private load(): void {
    try {
      const o = JSON.parse(fs.readFileSync(this.file, "utf8"));
      this.stats = o.stats ?? {};
      this.log = Array.isArray(o.log) ? o.log : [];
      if (typeof o.since === "number") this.since = o.since;
    } catch {
      /* файла ещё нет */
    }
  }
  private save(): void {
    if (!this.dirty) return;
    this.dirty = false;
    try {
      fs.writeFileSync(this.file, JSON.stringify({ since: this.since, stats: this.stats, log: this.log.slice(-LOG_MAX) }));
    } catch (err) {
      console.log("[lab-test] не удалось сохранить:", (err as Error).message);
    }
  }
  private st(vid: string): VStat {
    return (this.stats[vid] ??= newStat());
  }

  /** Новый сигнал базовой ячейки: заводим позицию для каждого подходящего варианта. */
  onSignal(idea: string, combo: string, market: TestMarket, side: Side, ask: number, bid0: number | null, now: number): void {
    for (const v of TEST_VARIANTS) {
      if (v.idea !== idea || v.combo !== combo) continue;
      if (v.cfg && !this.deps.cfgFilter(v.cfg)) continue;
      const key = `${v.id}|${market.eventSlug}|${side}`;
      if (this.pos.has(key)) continue;
      const tokenId = side === "Up" ? market.upTokenId : market.downTokenId;
      const p: Pos = {
        vid: v.id, slug: market.eventSlug, coin: market.coin, side, tokenId, closeMs: market.closeTimeMs, sigTs: now, sigAsk: ask,
        phase: "wait", stage: v.entry.k === "vyn" ? 0 : 1, lowBid: null, trail: [],
      };
      this.st(v.id).signals++;
      this.pos.set(key, p);
      if (v.entry.k === "now") this.open(v, p, ask, bid0, now);
    }
    this.dirty = true;
  }

  private open(v: TestVariant, p: Pos, ask: number, bid: number | null, now: number): void {
    p.phase = "open";
    p.buyTs = now;
    p.entry = ask;
    p.buyBid = bid;
    p.minBid = bid ?? ask;
    const s = v.stop;
    if (s) {
      let lv: number;
      if (s.u === "pct") lv = ask * (1 - s.v / 100);
      else if (s.u === "c") lv = ask - s.v / 100;
      else if (s.u === "abs") lv = s.v / 100;
      else lv = ask - (s.v * ask) / 5; // «−$X на ставку $5»: доля цены, как на странице
      p.level = Math.max(0.01, Math.round(lv * 100) / 100);
    }
    if (v.take) p.takeLevel = Math.min(0.99, Math.round((ask + v.take.c / 100) * 100) / 100);
    this.st(v.id).entered++;
    this.st(v.id).entrySum += ask;
    console.log(`[lab-test] ${v.id} вход ${p.coin} ${p.side} по ${ask.toFixed(3)}${p.level ? ` стоп ≤${p.level.toFixed(2)}` : ""} ${p.slug}`);
  }

  private valAt(trail: Array<[number, number]>, t: number): number | null {
    let r: number | null = null;
    for (const [ts, b] of trail) {
      if (ts <= t) r = b;
      else break;
    }
    return r;
  }

  /** Каждое обновление цены токена. */
  onTick(market: TestMarket, side: Side, bid: number | null, ask: number | null, now: number): void {
    if (this.pos.size === 0) return;
    const left = (market.closeTimeMs - now) / 1000;
    for (const v of TEST_VARIANTS) {
      const p = this.pos.get(`${v.id}|${market.eventSlug}|${side}`);
      if (!p || p.phase === "closed" || p.phase === "never") continue;
      if (bid === null) continue;
      if (p.phase === "wait") {
        this.tickWait(v, p, bid, ask, left, now);
        continue;
      }
      // позиция открыта
      p.trail.push([now, bid]);
      while (p.trail.length > 1 && now - p.trail[0][0] > TRAIL_MS) p.trail.shift();
      if (p.minBid === undefined || bid < p.minBid) p.minBid = bid;
      const entry = p.entry as number;
      // «падение bid за 5с до просадки −10¢/−20¢» — как колонки fs10/fs20 на странице
      for (const c of [10, 20] as const) {
        const key = c === 10 ? "fs10" : "fs20";
        if (p[key] === undefined && bid <= entry - c / 100 + 1e-9) {
          const prev = this.valAt(p.trail, now - 5000);
          p[key] = prev !== null ? prev - bid : null;
        }
      }
      if (p.takeLevel !== undefined && bid >= p.takeLevel - 1e-9) {
        this.exitPos(v, p, "take", bid, p.takeLevel, now);
        continue;
      }
      if (p.level !== undefined && !p.fsHeld && bid <= p.level + 1e-9) {
        const s = v.stop as StopRule;
        if (s.fast) {
          const dist = entry - p.level;
          const val = dist >= 0.2 - 1e-9 ? p.fs20 : dist >= 0.1 - 1e-9 ? p.fs10 : undefined;
          if (val !== undefined && val !== null && val < s.fast - 1e-9) {
            p.fsHeld = true;
            this.st(v.id).holdFs++;
            continue;
          }
        }
        this.exitPos(v, p, "stop", bid, p.level, now);
      }
    }
  }

  private tickWait(v: TestVariant, p: Pos, bid: number, ask: number | null, left: number, now: number): void {
    const e = v.entry;
    if (e.k === "now") return;
    if (left < MIN_LEFT) {
      p.phase = "never";
      this.st(v.id).skipped++;
      this.dirty = true;
      return;
    }
    if (e.k === "vyn" && p.stage === 0) {
      if (bid >= e.peak / 100 - 1e-9) p.stage = 1;
      return;
    }
    if (p.stage === 1) {
      if (bid <= e.low / 100 + 1e-9) {
        p.stage = 2;
        p.lowBid = bid;
      }
      return;
    }
    if (bid < e.buy / 100 - 1e-9 || ask === null || ask <= bid) return;
    if (ask > Math.min(0.99, (e.buy + ASK_GAP) / 100) + 1e-9) return;
    this.open(v, p, ask, bid, now);
  }

  private exitPos(v: TestVariant, p: Pos, kind: "stop" | "take", bidHit: number, level: number, now: number): void {
    const s = this.st(v.id);
    p.phase = "closed";
    p.exit = { kind, ts: now, bidHit };
    if (kind === "stop") s.stops++;
    else s.takes++;
    const ex = p.exit;
    void (async () => {
      const bids = await this.deps.fetchBids(p.tokenId);
      if (bids !== null) {
        let leftSh = SHARES;
        let sum = 0;
        for (const [px, sz] of bids) {
          const t = Math.min(leftSh, sz);
          sum += t * px;
          leftSh -= t;
          if (leftSh <= 1e-9) break;
        }
        ex.depthOk = leftSh <= 1e-9;
        ex.vwap = Math.round(((sum + (ex.depthOk ? 0 : leftSh * 0.01)) / SHARES) * 10000) / 10000;
      }
    })();
    setTimeout(() => (ex.bid2 = this.deps.getBid(p.tokenId)), 2000);
    setTimeout(() => (ex.bid5 = this.deps.getBid(p.tokenId)), 5000);
    console.log(`[lab-test] ${v.id} ${kind === "stop" ? "СТОП" : "ТЕЙК"} ${p.coin} ${p.side}: bid ${bidHit.toFixed(2)} (уровень ${level.toFixed(2)})`);
    this.dirty = true;
  }

  /** Рынок закрылся. */
  onSettle(slug: string, winner: Side): void {
    for (const [key, p] of [...this.pos]) {
      if (p.slug !== slug) continue;
      this.pos.delete(key);
      const v = TEST_VARIANTS.find((x) => x.id === p.vid);
      if (!v) continue;
      const s = this.st(v.id);
      if (p.phase === "wait") {
        s.skipped++;
        continue;
      }
      if (p.phase === "never" || p.entry === undefined) continue;
      const won = p.side === winner;
      const cost = SHARES * p.entry;
      const hold = (won ? SHARES : 0) - cost;
      let pnl = hold;
      let exitTxt = "до конца";
      if (p.exit) {
        const px = p.exit.vwap ?? p.exit.bidHit;
        pnl = SHARES * px - cost;
        exitTxt = `${p.exit.kind === "stop" ? "стоп" : "тейк"}: bid ${p.exit.bidHit.toFixed(2)} → продали по ${px.toFixed(3)}${p.exit.depthOk === false ? " (стакана не хватило)" : ""}`;
        if (p.exit.kind === "stop") {
          if (p.level !== undefined) {
            const slip = (p.level - px) * 100;
            s.slipN++;
            s.slipSum += slip;
            if (slip > s.slipMax) s.slipMax = slip;
          }
          if (p.exit.depthOk === false) s.depthFail++;
          if (won) s.stopsFalse++;
        }
      }
      s.pnl += pnl;
      s.pnlNoStop += hold;
      if (pnl > 0) s.wins++;
      this.log.push({ ts: p.buyTs ?? p.sigTs, vid: v.id, slug, coin: p.coin, side: p.side, entry: p.entry, exit: exitTxt, pnl, pnlHold: hold, won });
      if (this.log.length > LOG_MAX * 2) this.log = this.log.slice(-LOG_MAX);
    }
    this.dirty = true;
  }

  reset(): void {
    this.stats = {};
    this.log = [];
    this.since = Date.now();
    this.dirty = true;
    this.save();
  }

  private m = (x: number): string => (x >= 0 ? "+" : "−") + "$" + Math.abs(x).toFixed(2);

  statusText(arg?: string): string {
    const days = ((Date.now() - this.since) / 86400000).toFixed(1);
    if (arg) {
      const id = arg.toUpperCase();
      const v = TEST_VARIANTS.find((x) => x.id === id);
      if (!v) return `Нет варианта ${arg}. Список: «лаб тест».`;
      const s = this.st(id);
      const trades = this.log.filter((x) => x.vid === id).slice(-10).reverse();
      let t = `🧪 ${id}: ${v.tsName}\n${ruleTxt(v)}\n5 акций, бумажный режим.\n` +
        `Сигналов ${s.signals}, входов ${s.entered}, без входа ${s.skipped}. Итог ${this.m(s.pnl)} (без стопа/тейка было бы ${this.m(s.pnlNoStop)}), побед ${s.entered ? Math.round((s.wins / s.entered) * 100) : 0}%.\n`;
      if (v.stop) t += `Стопов ${s.stops} (рынок потом выиграл: ${s.stopsFalse}), не сработало из-за «резкого падения» ${s.holdFs}. Проскальзывание стопа: ${s.slipN ? `средн. ${(s.slipSum / s.slipN).toFixed(1)}¢, макс ${s.slipMax.toFixed(0)}¢` : "—"}, стакана не хватило ${s.depthFail}.\n`;
      if (v.take) t += `Тейков ${s.takes}.\n`;
      t += trades.length ? "Последние сделки:\n" + trades.map((x) => `${new Date(x.ts).toISOString().slice(5, 16).replace("T", " ")} ${x.coin} ${x.side} по ${x.entry.toFixed(3)} · ${x.exit} · ${this.m(x.pnl)}`).join("\n") : "Сделок ещё нет.";
      return t;
    }
    const lines = TEST_VARIANTS.map((v) => {
      const s = this.st(v.id);
      const base = `${v.id} ${v.tsName.replace("Справедливая цена", "СЦ").replace("Импульс 0.03% за", "Имп")} — ${ruleTxt(v)}`;
      const body = `сигн ${s.signals}, вход ${s.entered}, ${this.m(s.pnl)}${s.entered ? ` (${Math.round((s.wins / s.entered) * 100)}% побед)` : ""}${v.stop && s.stops ? `, стопов ${s.stops}, проскальз. ${s.slipN ? (s.slipSum / s.slipN).toFixed(1) + "¢" : "—"}` : ""}`;
      return `${base}\n   ${body}`;
    });
    const tot = Object.values(this.stats).reduce((a, s) => a + s.pnl, 0);
    return `🧪 LAB ТЕСТ (бумага, ничего не покупает) — идёт ${days} дн.\n\n${lines.join("\n")}\n\nСумма по всем: ${this.m(tot)}.\n«лаб тест T01» — подробности по варианту, «лаб тест сброс» — начать заново.`;
  }
}
