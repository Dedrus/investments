// Идентификатор для корпоративных облигаций — TQCB
// Идентификатор для ОФЗ — TQOB

const moexColumnKeys = {
    lastPrice: "LAST",
    tickerName: "SECNAME",
    shortName: "SHORTNAME",
    lotValue: "LOTVALUE", // номинальная стоимость лота
    couponValue: "COUPONVALUE",
    nextCoupon: "NEXTCOUPON",
    nkd: "ACCRUEDINT", // НКД на дату расчетов
    matDate: "MATDATE", // Дата погашения
    couponPeriod: "COUPONPERIOD", // длительность купона
    buybackPrice: "BUYBACKPRICE", // цена оферты
    couponPercent: "COUPONPERCENT",
    offerDate: "OFFERDATE",  // дата оферты
    duration: "DURATION",  //  дюрация, дней
    yieldToOffer: "YIELDTOOFFER",  // доходность к оферте
    effectiveYield: "EFFECTIVEYIELD",  // эффективная доходность,
    lCurrentPrice: "LCURRENTPRICE", // цена
};
const maxLockAttempts = 25;
const maxFetchAttempts = 3;
const cryptoBoardId = "__crypto"; // зарезервированная boardId для кэшей крипты
function getMoexBondField(ticker, boardId, fieldName) {
    if (boardId === undefined || boardId === null) {
        throw new Error("Provide board id");
    }
    return getMoexBond(ticker, boardId)[fieldName];
}

function getMoexShareField(ticker, boardId, fieldName) {
    if (boardId === undefined || boardId === null) {
        // режим по умолчанию для акций, для ETF другой
        boardId = "TQBR";
    }
    return getMoexShare(ticker, boardId)[fieldName];
}

function getMoexShare(ticker, boardId) {
    const cached = getCachedTicker(ticker, boardId);
    if (cached) {
        return cached;
    }
    return getAndCacheMoexShareData(ticker, boardId);
}

function getMoexBond(ticker, boardId) {
    const cached = getCachedTicker(ticker, boardId);
    if (cached) {
        return cached;
    }
    return getAndCacheMoexBondData(ticker, boardId);
}

function getCryptoPriceUsd(ticker) {
    let cached = getCachedTicker(ticker, cryptoBoardId);
    if (cached) {
        return cached.lastPrice;
    }
    const lockKey = getLockKey(ticker, cryptoBoardId);
    const cache = getUserCache();

    for (let i = 0; i < maxLockAttempts; i++) {
        const lock = cache.get(lockKey);
        if (!lock) {
            try {
                // Устанавливаем блокировку на 30 секунд
                cache.put(lockKey, "locked", 30);
                const url = "https://cryptoprices.cc/" + ticker;
                const httpResponse = fetchWithRetries(ticker, cryptoBoardId, url);
                const result = httpResponse.getContentText().trim();
                putTickerToCache(ticker, cryptoBoardId, { lastPrice: result });
                return result;
            } finally {
                // Снимаем блокировку
                cache.remove(lockKey);
            }
        }
        sleep(1000);
        // после слипа проверяем вдруг кто-то загрузил
        cached = getCachedTicker(ticker, boardId);
        if (cached) {
            return cached;
        }
    }
    throw new Error(`Could not get value for crypto ${ticker}`);
}

function getAndCacheMoexBondData(ticker, boardId) {
    const lockKey = getLockKey(ticker, boardId);
    const cache = getUserCache();

    for (let i = 0; i < maxLockAttempts; i++) {
        const lock = cache.get(lockKey);
        if (!lock) {
            try {
                // Устанавливаем блокировку на 30 секунд
                cache.put(lockKey, "locked", 30);
                const url = getMoexBondUrl(ticker, boardId);
                const httpResponse = fetchWithRetries(ticker, boardId, url);
                const json = JSON.parse(httpResponse.getContentText());
                const result = parseMoexBond(json);
                putTickerToCache(ticker, boardId, result);
                return result;
            } finally {
                // Снимаем блокировку
                cache.remove(lockKey);
            }
        }
        sleep(1000);

        // после слипа проверяем вдруг кто-то загрузил
        const cached = getCachedTicker(ticker, boardId);
        if (cached) {
            return cached;
        }
    }
    throw new Error(`Could not get value for bond ${ticker} ${boardId}`);
}

function getAndCacheMoexShareData(ticker, boardId) {
    const lockKey = getLockKey(ticker, boardId);
    const cache = getUserCache();

    for (let i = 0; i < maxLockAttempts; i++) {
        const lock = cache.get(lockKey);
        if (!lock) {
            try {
                // Устанавливаем блокировку на 30 секунд
                cache.put(lockKey, "locked", 30);
                const url = getMoexShareUrl(ticker, boardId);
                const httpResponse = fetchWithRetries(ticker, boardId, url);
                const json = JSON.parse(httpResponse.getContentText());
                const result = parseMoexShare(json);
                putTickerToCache(ticker, boardId, result);
                return result;
            } finally {
                // Снимаем блокировку
                cache.remove(lockKey);
            }
        }
        sleep(1000);
        // после слипа проверяем вдруг кто-то загрузил
        const cached = getCachedTicker(ticker, boardId);
        if (cached) {
            return cached;
        }
    }
    throw new Error(`Could not get value for share ${ticker} ${boardId}`);
}

function fetchWithRetries(ticker, boardId, url) {
    for (let i = 0; i < maxFetchAttempts; i++) {
        try {
            const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
            if (response.getResponseCode() === 200) {
                return response;
            }
        } catch (error) {
            console.error(error);
        }
        sleep(1000 * (i + 1));
    }
    throw new Error(`Could not fetch ${ticker} ${boardId} from ${url}`);
}


function parseMoexShare(json) {
    const lastPrice = parseMoexColumn(json.marketdata, moexColumnKeys.lastPrice);
    const shortName = parseMoexColumn(json.securities, moexColumnKeys.shortName);

    return {
        lastPrice,
        shortName,
    };
}

function parseMoexBond(json) {
    const shortName = parseMoexColumn(json.securities, moexColumnKeys.shortName);
    const tickerName = parseMoexColumn(json.securities, moexColumnKeys.tickerName);
    const lotValue = parseMoexColumn(json.securities, moexColumnKeys.lotValue);
    const couponValue = parseMoexColumn(json.securities, moexColumnKeys.couponValue);
    const nextCoupon = parseMoexColumn(json.securities, moexColumnKeys.nextCoupon);
    const nkd = parseMoexColumn(json.securities, moexColumnKeys.nkd);
    const matDate = parseMoexColumn(json.securities, moexColumnKeys.matDate);
    const couponPeriod = parseMoexColumn(json.securities, moexColumnKeys.couponPeriod);
    const buybackPrice = parseMoexColumn(json.securities, moexColumnKeys.buybackPrice);
    const couponPercent = parseMoexColumn(json.securities, moexColumnKeys.couponPercent);
    const offerDate = parseMoexColumn(json.securities, moexColumnKeys.offerDate);

    const duration = parseMoexColumn(json.marketdata, moexColumnKeys.duration);
    const yieldToOffer = parseMoexColumn(json.marketdata, moexColumnKeys.yieldToOffer);
    let lastPrice = parseMoexColumn(json.marketdata, moexColumnKeys.lCurrentPrice);
    if (!lastPrice) {
        lastPrice = parseMoexColumn(json.marketdata, moexColumnKeys.lastPrice);
    }

    const effectiveYield = parseMoexColumn(json.marketdata_yields, moexColumnKeys.effectiveYield);

    return {
        lastPrice,
        shortName,
        tickerName,
        lotValue,
        couponValue,
        nextCoupon,
        nkd,
        matDate,
        couponPeriod,
        buybackPrice,
        couponPercent,
        offerDate,
        duration,
        yieldToOffer,
        effectiveYield,
    };
}

function parseMoexColumn(data, moexColumnKey) {
    const valueIndex = data.columns.findIndex(value => value === moexColumnKey);
    return data.data[0][valueIndex];
}

function getLockKey(ticker, boardId) {
    return `lock_${boardId}_${ticker}`;
}

function getCachedTicker(ticker, boardId) {
    const key = getCacheKey(ticker, boardId);
    const cached = getUserCache().get(key);
    return cached ? JSON.parse(cached) : null;
}

function putTickerToCache(ticker, boardId, result) {
    const key = getCacheKey(ticker, boardId);
    getUserCache().put(key, JSON.stringify(result), 60 * 30);
}

function getCacheKey(ticker, boardId) {
    return `${boardId}_${ticker}`;
}

function getUserCache() {
    return CacheService.getUserCache();
}

function getMoexShareUrl(ticker, boardId) {
    return "https://iss.moex.com/iss/engines/stock/markets/shares/boards/"
        + boardId
        + "/securities/"
        + ticker
        + ".json?iss.meta=off&iss.only=marketdata,securities&marketdata.columns=LAST&securities.columns=SHORTNAME";
}

function getMoexBondUrl(ticker, boardId) {
    return "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/"
        + boardId
        + "/securities/"
        + ticker +
        ".json?iss.meta=off&iss.only=marketdata,securities,marketdata_yields" +
        "&marketdata.columns=LAST,DURATION,YIELDTOOFFER,LCURRENTPRICE" +
        "&securities.columns=SHORTNAME,SECNAME,LOTVALUE,COUPONVALUE,NEXTCOUPON,ACCRUEDINT,MATDATE,COUPONPERIOD,BUYBACKPRICE,COUPONPERCENT,OFFERDATE" +
        "&marketdata_yields.columns=EFFECTIVEYIELD";
}

function sleep(milliseconds) {
    Utilities.sleep(milliseconds);
}

function clearCache(ticker, boardId) {
    const cacheKey = getCacheKey(ticker, boardId);
    getUserCache().remove(cacheKey);
}

/**
 * Функция создает меню в Google Таблице при открытии.
 * onOpen() — специальное имя, Google вызывает его автоматически.
 */
function onOpen() {
    const ui = SpreadsheetApp.getUi();

    ui.createMenu("📈 Инвестиции")
        .addItem("Пересчитать ошибки", "forceRecalculation")
        .addToUi();
}

/**
 * Функция для пересчета упавших ячеек
 */
function forceRecalculation() {
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const sheets = spreadsheet.getSheets();
    let totalFixed = 0;

    const forceRecalculateCell = (cell) => {
        const formula = cell.getFormula();
        if (!formula) return false;

        // 1. Сохраняем оригинальную формулу
        const originalFormula = formula;

        // 2. Временно заменяем на простое значение
        cell.setValue("⌛ Обновление...");
        SpreadsheetApp.flush(); // Принудительно применяем изменения

        // 3. Возвращаем оригинальную формулу
        cell.setFormula(originalFormula);

        return true;
    };

    sheets.forEach(sheet => {
        if (sheet.isSheetHidden()) return;

        const range = sheet.getDataRange();
        const formulas = range.getFormulas();
        const values = range.getDisplayValues();

        for (let row = 0; row < formulas.length; row++) {
            for (let col = 0; col < formulas[row].length; col++) {

                const cell = range.getCell(row + 1, col + 1);
                const displayedValue = values[row][col];
                const formula = formulas[row][col];

                if (formula && displayedValue.startsWith("#") && formula.includes("getMoex")) {
                    if (forceRecalculateCell(cell)) {
                        totalFixed++;
                    }
                }
            }
        }
    });

    SpreadsheetApp.getUi().alert(`✅ Успешно перезапущено ${totalFixed} ячеек`);
}

function test() {
    const ticker = "SU26248RMFS3";
    const board = "TQOB";
    clearCache(ticker, board);
    const result = getMoexBond(ticker, board);
    const cachedResult = getMoexBond(ticker, board);
    const lastPrice = getMoexBondField(ticker, board, "lastPrice");

    const shareTicker = "SBER";
    const shareBoardId = "TQBR";
    clearCache(shareTicker, shareBoardId);
    const shareName = getMoexShareField(shareTicker, shareBoardId, "shortName");
    // проверка default Борды
    const sharePrice = getMoexShareField(shareTicker, null, "lastPrice");

    const cryptoTicker = "BTC";
    clearCache(ticker, cryptoBoardId);
    const cryptoPrice = getCryptoPriceUsd(cryptoTicker);
    return result;
}