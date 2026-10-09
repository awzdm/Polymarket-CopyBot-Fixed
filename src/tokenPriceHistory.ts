/**
 * История цен ТОКЕНОВ (не монет) в памяти — нужна основному парсеру (Research.ts)
 * для пассивных микро-метрик: скорость цены токена, скользящая средняя, частота
 * обновлений, асимметрия сторон. Ничего не решает при входе, только логируется.
 *
 * Хранит для каждого токена последние ~10 минут обновлений цены (время + цена).
 */

interface Sample {
  ts: number;
  price: number;
}

const RETENTION_MS = 10 * 60 * 1000;

class TokenPriceHistory {
  private buffers = new Map<string, Sample[]>();

  /** Записывает очередное обновление цены токена. */
  recordUpdate(tokenId: string, price: number, ts: number): void {
    let buf = this.buffers.get(tokenId);
    if (!buf) {
      buf = [];
      this.buffers.set(tokenId, buf);
    }
    buf.push({ ts, price });

    // выбрасываем всё, что старше окна хранения
    const cutoff = ts - RETENTION_MS;
    let drop = 0;
    while (drop < buf.length && buf[drop].ts < cutoff) drop++;
    if (drop > 0) buf.splice(0, drop);
  }

  /** Освобождает память по токену, которого больше нет в наблюдении. */
  forget(tokenId: string): void {
    this.buffers.delete(tokenId);
  }

  /** Последняя известная цена токена. */
  getLatestPrice(tokenId: string): number | null {
    const buf = this.buffers.get(tokenId);
    if (!buf || buf.length === 0) return null;
    return buf[buf.length - 1].price;
  }

  /** Цена токена на момент ts (последнее обновление не позже ts). */
  getPriceAt(tokenId: string, ts: number): number | null {
    const buf = this.buffers.get(tokenId);
    if (!buf || buf.length === 0) return null;
    if (buf[0].ts > ts) return null;

    // бинарный поиск последнего элемента с ts <= искомого
    let lo = 0;
    let hi = buf.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (buf[mid].ts <= ts) lo = mid;
      else hi = mid - 1;
    }
    return buf[lo].price;
  }

  /** Сколько обновлений цены пришло по токену за последние windowMs до момента now. */
  getUpdateCount(tokenId: string, now: number, windowMs: number): number {
    const buf = this.buffers.get(tokenId);
    if (!buf) return 0;
    const from = now - windowMs;
    let count = 0;
    for (let i = buf.length - 1; i >= 0; i--) {
      if (buf[i].ts <= from) break;
      if (buf[i].ts <= now) count++;
    }
    return count;
  }

  /** Средняя цена токена за последние windowMs до момента now (null, если обновлений не было). */
  getMovingAverage(tokenId: string, now: number, windowMs: number): number | null {
    const buf = this.buffers.get(tokenId);
    if (!buf || buf.length === 0) return null;
    const from = now - windowMs;
    let sum = 0;
    let count = 0;
    for (let i = buf.length - 1; i >= 0; i--) {
      if (buf[i].ts <= from) break;
      if (buf[i].ts <= now) {
        sum += buf[i].price;
        count++;
      }
    }
    return count > 0 ? sum / count : null;
  }

  /** Изменение цены токена со знаком: цена сейчас минус цена windowMs назад. */
  getSignedMoveOverWindow(tokenId: string, now: number, windowMs: number): number | null {
    const end = this.getPriceAt(tokenId, now);
    const start = this.getPriceAt(tokenId, now - windowMs);
    if (end === null || start === null) return null;
    return end - start;
  }
}

export const tokenPriceHistory = new TokenPriceHistory();
