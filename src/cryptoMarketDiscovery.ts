/**
 * Модуль 1: поиск ВСЕХ активных крипто "Up or Down" рынков на Polymarket
 * (5-минутки, 15-минутки, часовые — любые монеты: BTC, ETH, SOL, XRP, DOGE и т.д.)
 *
 * Логика такая же, как в marketDiscovery.ts (погодный бот): тянем события
 * через Gamma API постранично, фильтруем по заголовку.
 *
 * Важная деталь по времени закрытия:
 *  - Если в заголовке ДВА времени ("1:50AM-1:55AM ET") — это диапазон,
 *    закрытие = второе время, длина окна = разница между ними.
 *  - Если в заголовке ОДНО время ("7PM ET") — это НАЧАЛО часового окна,
 *    закрытие = это время + 60 минут.
 *  - Плюс на всякий случай сверяем с полем endDate самого события, если
 *    Gamma API его отдаёт — при первом тестовом запуске увидим в логах,
 *    совпадает ли оно с тем, что мы посчитали из заголовка.
 */

const GAMMA_HOST = "https://gamma-api.polymarket.com";

export interface CryptoUpDownMarket {
  coin: string;              // "Bitcoin", "Ethereum", "XRP", ...
  eventSlug: string;
  title: string;
  windowMinutes: number;     // 5, 15, 60...
  closeTimeMs: number;       // когда закрывается окно (unix ms, наш расчёт из заголовка)
  closeTimeFromApiMs: number | null; // endDate от самой Gamma API, если есть — для сверки
  upTokenId: string;
  downTokenId: string;
}

// ET сейчас (август) — летнее время, UTC-4. Зимой (примерно ноябрь-март) будет UTC-5.
// Это единственное, что может "поплыть" по сезону — если в ноябре расчёты времени
// начнут расходиться на 1 час, сюда и смотреть в первую очередь.
const ET_OFFSET_HOURS = -4;

function parseTimeToken(monthDay: string, timeStr: string, year: number): Date {
  // timeStr вида "1:50AM" или "7PM"
  const m = timeStr.match(/^(\d{1,2})(?::(\d{2}))?([AP]M)$/i);
  if (!m) throw new Error(`Не удалось распарсить время: "${timeStr}"`);
  let hour = Number(m[1]);
  const minute = m[2] ? Number(m[2]) : 0;
  const ampm = m[3].toUpperCase();
  if (ampm === "PM" && hour !== 12) hour += 12;
  if (ampm === "AM" && hour === 12) hour = 0;

  // monthDay вида "August 28"
  const md = new Date(`${monthDay} ${year} 00:00:00 UTC`);
  const utcMs =
    Date.UTC(md.getUTCFullYear(), md.getUTCMonth(), md.getUTCDate(), hour, minute, 0) -
    ET_OFFSET_HOURS * 3600 * 1000;
  return new Date(utcMs);
}

/**
 * Возвращает { closeTimeMs, windowMinutes, coin } или null, если заголовок
 * не похож на крипто up/down рынок.
 */
export function parseUpDownTitle(title: string): {
  coin: string;
  closeTimeMs: number;
  windowMinutes: number;
} | null {
  const rangeMatch = title.match(
    /^(\w+) Up or Down - ([A-Za-z]+ \d+), (\d{1,2}(?::\d{2})?[AP]M)-(\d{1,2}(?::\d{2})?[AP]M) ET/,
  );
  if (rangeMatch) {
    const [, coin, monthDay, startStr, endStr] = rangeMatch;
    const year = new Date().getFullYear();
    const start = parseTimeToken(monthDay, startStr, year);
    let end = parseTimeToken(monthDay, endStr, year);
    // Окно может переходить через полночь (например "11:55PM-12:00AM ET") —
    // в этом случае конец на самом деле на следующий день.
    if (end.getTime() <= start.getTime()) {
      end = new Date(end.getTime() + 24 * 3600 * 1000);
    }
    const windowMinutes = Math.round((end.getTime() - start.getTime()) / 60000);
    return { coin, closeTimeMs: end.getTime(), windowMinutes };
  }

  const singleMatch = title.match(
    /^(\w+) Up or Down - ([A-Za-z]+ \d+), (\d{1,2}(?::\d{2})?[AP]M) ET$/,
  );
  if (singleMatch) {
    const [, coin, monthDay, startStr] = singleMatch;
    const year = new Date().getFullYear();
    const start = parseTimeToken(monthDay, startStr, year);
    const closeTimeMs = start.getTime() + 60 * 60 * 1000; // час после начала
    return { coin, closeTimeMs, windowMinutes: 60 };
  }

  return null;
}

// Polymarket недавно поменял API: старый способ (tag_slug + offset-пагинация)
// теперь отдаёт 422. Рабочий задокументированный способ для этих рынков —
// запрос по series_slug: у каждой монеты/таймфрейма есть своя серия
// (например "btc-up-or-down-5m"), внутри которой лежат все её окна разом,
// без пагинации вообще.
//
// Список монет ниже — стартовый набор по тем, что видны в CSV трейдера
// (Bitcoin, Solana, XRP, Dogecoin) + Ethereum. Если у Polymarket появится
// рынок по монете, которой здесь нет, — просто добавим её тикер в список.
const COIN_TICKERS: Record<string, string> = {
  btc: "Bitcoin",
  eth: "Ethereum",
  sol: "Solana",
  xrp: "XRP",
  doge: "Dogecoin",
};

// Суффиксы серий, которые пробуем для каждой монеты. Если какой-то суффикс
// не существует для монеты — API просто вернёт пустой список, ничего не сломается.
// Для часовых рынков пробуем несколько вариантов названия, т.к. точное имя
// серии не задокументировано (по данным с самого Polymarket они называются
// "Up or Down Hourly" в интерфейсе, но слаг серии может отличаться).
const SERIES_SUFFIXES = [
  "up-or-down-5m",
  "up-or-down-1h",
  "up-or-down-hourly",
  "updown-hourly",
];

async function fetchEventsBySeriesSlug(seriesSlug: string): Promise<any[]> {
  const url = `${GAMMA_HOST}/events?series_slug=${seriesSlug}&closed=false&limit=500&order=endDate&ascending=true`;
  const resp = await fetch(url);
  if (!resp.ok) {
    // Не бросаем ошибку на всю пачку — просто эта серия не существует
    // или временно недоступна, пробуем следующую.
    console.warn(`  [${seriesSlug}] Gamma API вернул ${resp.status} — пропускаем`);
    return [];
  }
  return (await resp.json()) as any[];
}

async function fetchActiveCryptoEvents(): Promise<any[]> {
  const allEvents: any[] = [];
  for (const [ticker, coinName] of Object.entries(COIN_TICKERS)) {
    for (const suffix of SERIES_SUFFIXES) {
      const seriesSlug = `${ticker}-${suffix}`;
      const events = await fetchEventsBySeriesSlug(seriesSlug);
      if (events.length > 0) {
        console.log(`  [${seriesSlug}] найдено событий: ${events.length}`);
      }
      allEvents.push(...events);
    }
  }
  return allEvents;
}

function extractOutcomeTokens(market: any): { upTokenId: string; downTokenId: string } | null {
  let outcomes: string[];
  let tokenIds: string[];
  try {
    outcomes = JSON.parse(market.outcomes ?? "[]");
    tokenIds = JSON.parse(market.clobTokenIds ?? "[]");
  } catch {
    return null;
  }
  if (outcomes.length !== 2 || tokenIds.length !== 2) return null;

  const upIdx = outcomes.findIndex((o) => /^up$/i.test(o.trim()));
  const downIdx = outcomes.findIndex((o) => /^down$/i.test(o.trim()));
  if (upIdx === -1 || downIdx === -1) return null;

  return { upTokenId: tokenIds[upIdx], downTokenId: tokenIds[downIdx] };
}

export async function discoverCryptoUpDownMarkets(): Promise<CryptoUpDownMarket[]> {
  const events = await fetchActiveCryptoEvents();
  const results: CryptoUpDownMarket[] = [];

  const now = Date.now();
  // Оставляем только то, что реально актуально прямо сейчас: не закрылось
  // больше 2 минут назад (небольшой запас на случай задержки резолва) и
  // закрывается не позже, чем через 36 часов. Это отсекает мусорные/старые
  // записи с "битыми" датами вроде декабрьских, которые видели в первом тесте.
  const MIN_CLOSE_MS = now - 2 * 60 * 1000;
  const MAX_CLOSE_MS = now + 36 * 3600 * 1000;

  for (const ev of events) {
    const title: string = ev.title ?? "";
    const parsed = parseUpDownTitle(title);
    if (!parsed) continue;
    if (parsed.closeTimeMs < MIN_CLOSE_MS || parsed.closeTimeMs > MAX_CLOSE_MS) continue;

    // У события обычно один вложенный market (бинарный Up/Down)
    const market = (ev.markets ?? [])[0];
    if (!market) continue;

    const tokens = extractOutcomeTokens(market);
    if (!tokens) continue;

    const closeTimeFromApiMs = ev.endDate ? new Date(ev.endDate).getTime() : null;

    results.push({
      coin: parsed.coin,
      eventSlug: ev.slug,
      title,
      windowMinutes: parsed.windowMinutes,
      closeTimeMs: parsed.closeTimeMs,
      closeTimeFromApiMs,
      upTokenId: tokens.upTokenId,
      downTokenId: tokens.downTokenId,
    });
  }

  results.sort((a, b) => a.closeTimeMs - b.closeTimeMs);
  return results;
}

// Тестовый запуск: npx tsx src/cryptoMarketDiscovery.ts
if (import.meta.url === `file://${process.argv[1]}`) {
  discoverCryptoUpDownMarkets().then((markets) => {
    console.log(`Найдено крипто up/down рынков: ${markets.length}`);

    const byWindow = new Map<number, number>();
    for (const m of markets) {
      byWindow.set(m.windowMinutes, (byWindow.get(m.windowMinutes) ?? 0) + 1);
    }
    console.log("По длительности окна (минут -> кол-во):", Object.fromEntries(byWindow));

    const coins = new Set(markets.map((m) => m.coin));
    console.log("Монеты:", [...coins].join(", "));

    console.log("\nБлижайшие к закрытию (5 штук):");
    for (const m of markets.slice(0, 5)) {
      const closeIn = Math.round((m.closeTimeMs - Date.now()) / 1000);
      const apiMatch =
        m.closeTimeFromApiMs !== null
          ? `${Math.round((m.closeTimeFromApiMs - m.closeTimeMs) / 1000)}с расхождение с endDate`
          : "endDate отсутствует в ответе API";
      console.log(
        `- [${m.coin} / ${m.windowMinutes}мин] "${m.title}" — закрытие через ${closeIn}с (${apiMatch})`,
      );
    }
  }).catch((err) => {
    console.error("Ошибка:", err);
    process.exit(1);
  });
}