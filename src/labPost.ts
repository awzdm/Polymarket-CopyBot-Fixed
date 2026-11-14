/**
 * ЛАБ: что происходит с токенами ПОСЛЕ закрытия окна и до резолва (v6).
 * Идея: топовые аккаунты покупают по 99–100¢ уже после 15:05, пока рынок ещё не resolved (резолв приходит через ~30–50 с).
 * Здесь только замер: пишем стаканы обоих токенов раз в ~3с после закрытия окна, момент резолва и итоговый ход монеты.
 * Торговлю не трогает. Отдельные файлы lab-post/post-ГГГГ-ММ-ДД.jsonl (одна строка = один рынок).
 */
import * as fs from "fs";
import * as path from "path";

export interface PostBook {
  bid: number | null;
  ask: number | null;
  bidSize: number;
  askSize: number;
}

export interface PostDeps {
  dir: string;
  fetchBook: (tokenId: string) => Promise<PostBook | null>;
  resolveWinner: (slug: string) => Promise<"Up" | "Down" | null>;
  /** движение монеты от открытия окна до сейчас, bps (вверх +); null — нет данных */
  coinMove: (coin: string, openMs: number) => number | null;
  /** то же, но на момент закрытия окна */
  coinMoveAt: (coin: string, openMs: number, atMs: number) => number | null;
}

interface Watch {
  slug: string;
  coin: string;
  wm: number;
  openMs: number;
  closeMs: number;
  up: string;
  dn: string;
  rows: (number | null)[][];
  nextBook: number;
  nextRes: number;
  busy: boolean;
  fin: number | null;
  win: 0 | 1 | null;
  resSec: number | null;
  doneAt: number;
}

const POLL_MS = 3000;
const RES_EVERY_MS = 6000;
const MAX_AFTER_MS = 420 * 1000;
const TAIL_AFTER_RES_MS = 6000;

export class PostWatcher {
  private w = new Map<string, Watch>();
  private timer: NodeJS.Timeout | null = null;
  constructor(private readonly d: PostDeps) {}

  start(): void {
    try { fs.mkdirSync(this.d.dir, { recursive: true }); } catch { /* ignore */ }
    if (!this.timer) this.timer = setInterval(() => this.tick(), 1000);
  }

  add(slug: string, coin: string, wm: number, closeMs: number, up: string, dn: string): void {
    if (this.w.has(slug)) return;
    if (closeMs + MAX_AFTER_MS < Date.now()) return;
    this.w.set(slug, {
      slug, coin, wm, openMs: closeMs - wm * 60000, closeMs, up, dn, rows: [],
      nextBook: closeMs, nextRes: closeMs + 8000, busy: false, fin: null, win: null, resSec: null, doneAt: 0,
    });
  }

  private tick(): void {
    const now = Date.now();
    for (const x of [...this.w.values()]) {
      if (now < x.closeMs) continue;
      if (x.fin === null) x.fin = this.d.coinMoveAt(x.coin, x.openMs, x.closeMs);
      const finished = (x.doneAt > 0 && now >= x.doneAt) || now > x.closeMs + MAX_AFTER_MS;
      if (finished && !x.busy) { this.flush(x); continue; }
      if (x.busy) continue;
      x.busy = true;
      void this.step(x, now).finally(() => { x.busy = false; });
    }
  }

  private async step(x: Watch, now: number): Promise<void> {
    try {
      if (now >= x.nextBook && x.doneAt === 0) {
        x.nextBook = now + POLL_MS;
        const [u, d] = await Promise.all([this.d.fetchBook(x.up), this.d.fetchBook(x.dn)]);
        const t = Math.round(((Date.now() - x.closeMs) / 1000) * 10) / 10;
        const cm = this.d.coinMove(x.coin, x.openMs);
        const r3 = (v: number | null | undefined): number | null => (v === null || v === undefined ? null : Math.round(v * 1000) / 1000);
        x.rows.push([
          t,
          r3(u?.bid), u ? Math.round(u.bidSize) : null, r3(u?.ask), u ? Math.round(u.askSize) : null,
          r3(d?.bid), d ? Math.round(d.bidSize) : null, r3(d?.ask), d ? Math.round(d.askSize) : null,
          cm === null ? null : Math.round(cm * 100) / 100,
        ]);
      }
      if (now >= x.nextRes && x.doneAt === 0) {
        x.nextRes = now + RES_EVERY_MS;
        const w = await this.d.resolveWinner(x.slug);
        if (w !== null) {
          x.win = w === "Up" ? 0 : 1;
          x.resSec = Math.round(((Date.now() - x.closeMs) / 1000) * 10) / 10;
          x.doneAt = Date.now() + TAIL_AFTER_RES_MS;
        }
      }
    } catch { /* замер best-effort */ }
  }

  private flush(x: Watch): void {
    this.w.delete(x.slug);
    if (x.rows.length === 0) return;
    try {
      const day = new Date(x.closeMs).toISOString().slice(0, 10);
      const rec = { v: 1, slug: x.slug, coin: x.coin, wm: x.wm, close: x.closeMs, fin: x.fin, win: x.win, res: x.resSec, rows: x.rows };
      fs.appendFileSync(path.join(this.d.dir, `post-${day}.jsonl`), JSON.stringify(rec) + "\n", "utf-8");
    } catch (err) {
      console.error("[lab-post] не удалось записать:", (err as Error).message);
    }
  }

  // ───────── отчёт ─────────
  report(days: number): string {
    const recs: any[] = [];
    try {
      const names = fs.readdirSync(this.d.dir).filter((n) => /^post-\d{4}-\d{2}-\d{2}\.jsonl$/.test(n)).sort().slice(-Math.max(1, days));
      for (const n of names) {
        for (const line of fs.readFileSync(path.join(this.d.dir, n), "utf-8").split("\n")) {
          if (!line) continue;
          try { recs.push(JSON.parse(line)); } catch { /* битая строка */ }
        }
      }
    } catch { /* нет папки */ }
    if (recs.length === 0) return `🕐 ЛАБ «после окна»: данных пока нет (запись идёт с момента запуска этой версии).`;
    const pq = (a: number[], q: number): string => (a.length ? a.slice().sort((x, y) => x - y)[Math.min(a.length - 1, Math.floor(q * a.length))].toFixed(0) : "—");
    const L: string[] = [];
    const res = recs.map((r) => r.res).filter((v) => typeof v === "number") as number[];
    L.push(`🕐 ЛАБ «после окна» — рынков записано: ${recs.length}, резолв пойман: ${res.length}`);
    L.push(`Сколько секунд после закрытия окна до резолва: быстрые 10% ≤ ${pq(res, 0.1)}с, медиана ${pq(res, 0.5)}с, медленные 10% ≥ ${pq(res, 0.9)}с`);
    L.push("");
    L.push("Покупаем токен той стороны, куда монета ушла к закрытию, по ask после закрытия (цены ≤ уровня, на ≥5 акций). Решение «кто лидер» — по ходу монеты (не по токену).");
    const levels = [0.999, 0.998, 0.995, 0.99, 0.98, 0.95];
    const buckets: [string, number, number][] = [["|ход| < 1 bps", 0, 1], ["1–2", 1, 2], ["2–4", 2, 4], ["> 4", 4, 1e9]];
    for (const [name, lo, hi] of buckets) {
      const set = recs.filter((r) => typeof r.fin === "number" && Math.abs(r.fin) >= lo && Math.abs(r.fin) < hi && r.win !== null);
      if (!set.length) continue;
      let wins = 0;
      const got: number[] = levels.map(() => 0);
      const pnl: number[] = levels.map(() => 0);
      const secs: number[][] = levels.map(() => []);
      for (const r of set) {
        const lead = r.fin >= 0 ? 0 : 1;
        if (r.win === lead) wins++;
        for (let li = 0; li < levels.length; li++) {
          for (const row of r.rows) {
            const ask = lead === 0 ? row[3] : row[7], sz = lead === 0 ? row[4] : row[8];
            if (ask !== null && ask !== undefined && ask <= levels[li] + 1e-9 && sz !== null && sz >= 5 && ask > 0) {
              got[li]++;
              pnl[li] += r.win === lead ? 5 * (1 - ask) : -5 * ask;
              secs[li].push(row[0]);
              break;
            }
          }
        }
      }
      L.push(`${name}: рынков ${set.length}, лидер выиграл ${(wins / set.length * 100).toFixed(1)}%`);
      levels.forEach((lv, li) => L.push(`   ask ≤ ${Math.round(lv * 1000) / 10}¢ нашёлся в ${got[li]} (${(got[li] / set.length * 100).toFixed(0)}%), на 5 акций ${pnl[li] >= 0 ? "+" : "−"}$${Math.abs(pnl[li]).toFixed(2)}, первая секунда медиана ${pq(secs[li], 0.5)}с`));
    }
    return L.join("\n");
  }
}
