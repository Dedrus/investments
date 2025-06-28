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
    prevPrice: "PREVPRICE" // предыдущая цена для фоллбэка
};
const maxLockAttempts = 10;
const maxFetchAttempts = 3;
const cryptoBoardId = "__crypto"; // зарезервированная boardId для кэшей крипты
function getMoexBondField(ticker, boardId, fieldName) {
    if (boardId === undefined || boardId === null) {
        throw new Error("Provide board id");
    }
    const data = getMoexBond(ticker, boardId);

    if (!(fieldName in data)) {
        throw new Error(`Invalid field name: ${fieldName}`);
    }

    return data[fieldName];
}

function getMoexShareField(ticker, boardId, fieldName) {
    if (boardId === undefined || boardId === null) {
        // режим по умолчанию для акций, для ETF другой
        boardId = "TQBR";
    }
    const data = getMoexShare(ticker, boardId);

    if (!(fieldName in data)) {
        throw new Error(`Invalid field name: ${fieldName}`);
    }

    return data[fieldName];
}

function getCryptoPriceUsd(ticker) {
    return fetchAndParseData(ticker, cryptoBoardId,
        (t) => "https://cryptoprices.cc/" + t,
        (content) => (content.trim()),
        "crypto data");
}

function getMoexShare(ticker, boardId) {
    return fetchAndParseData(ticker, boardId, getMoexShareUrl, parseMoexShare, "share data");
}

function getMoexBond(ticker, boardId) {
    return fetchAndParseData(ticker, boardId, getMoexBondUrl, parseMoexBond, "bond data");
}

function fetchAndParseData(ticker, boardId, urlBuilder, responseContentTextParserFn, errorContextType) {
    const cacheHit = getCachedTicker(ticker, boardId);
    if (cacheHit) {
        return cacheHit;
    }
    const lockKey = getLockKey(ticker, boardId);

    for (let i = 0; i < maxLockAttempts; i++) {
        // каждый раз берем кэш заново, наверняка функция не получит изменения других функций в своем объекте
        const cache = getUserCache();
        const lock = cache.get(lockKey);
        if (!lock) {
            try {
                cache.put(lockKey, "locked", 20);
                if(i == 1) {
                  // если мы тут значит кто-то до этого брал блокировку и возможно все загрузил
                  let multiThreadCache = getCachedTicker(ticker, boardId);
                  if (multiThreadCache) {
                      return multiThreadCache;
                  }
                  // даем последний шанс
                  sleep(10000);
                  multiThreadCache = getCachedTicker(ticker, boardId);
                  if (multiThreadCache) {
                      return multiThreadCache;
                  }
                }
                const url = urlBuilder(ticker, boardId);
                const httpResponse = fetchWithRetries(ticker, boardId, url);
                const result = responseContentTextParserFn(httpResponse.getContentText());
                putTickerToCache(ticker, boardId, result);
                return result;
            } finally {
                cache.remove(lockKey);
            }
        }
        sleep(1000 + Math.random() * 1000);

        // после слипа проверяем вдруг кто-то загрузил
        const multiThreadCache = getCachedTicker(ticker, boardId);
        if (multiThreadCache) {
            return multiThreadCache;
        }
    }

    throw new Error(`Could not get ${errorContextType} for ${ticker} ${boardId}`);
}

function fetchWithRetries(ticker, boardId, url) {
    let lastError;
    for (let i = 0; i < maxFetchAttempts; i++) {
        try {
            const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
            const responseCode = response.getResponseCode();
            if (responseCode === 200) {
                return response;
            }
            lastError = `HTTP ${responseCode}`;
        } catch (error) {
            console.error(error);
            lastError = error;
        }
        sleep(1000 * (i + 1));
    }
    throw new Error(`Could not fetch ${ticker} ${boardId} from ${url} last error: ${lastError}`);
}


function parseMoexShare(responseTextContent) {
    const json = JSON.parse(responseTextContent);
    const lastPrice = parseMoexColumn(json.marketdata, moexColumnKeys.lastPrice)
      || parseMoexColumn(json.securities, moexColumnKeys.prevPrice);
    const shortName = parseMoexColumn(json.securities, moexColumnKeys.shortName);

    return {
        lastPrice,
        shortName,
    };
}

function parseMoexBond(responseTextContent) {
    const json = JSON.parse(responseTextContent);
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
    const lastPrice = parseMoexColumn(json.marketdata, moexColumnKeys.lCurrentPrice) 
      || parseMoexColumn(json.marketdata, moexColumnKeys.lastPrice)
      || parseMoexColumn(json.securities, moexColumnKeys.prevPrice);

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
        + ".json?iss.meta=off&iss.only=marketdata,securities&marketdata.columns=LAST&securities.columns=SHORTNAME,PREVPRICE";
}

function getMoexBondUrl(ticker, boardId) {
    return "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/"
        + boardId
        + "/securities/"
        + ticker +
        ".json?iss.meta=off&iss.only=marketdata,securities,marketdata_yields" +
        "&marketdata.columns=LAST,DURATION,YIELDTOOFFER,LCURRENTPRICE" +
        "&securities.columns=SHORTNAME,SECNAME,LOTVALUE,COUPONVALUE,NEXTCOUPON,ACCRUEDINT,MATDATE,COUPONPERIOD,BUYBACKPRICE,COUPONPERCENT,OFFERDATE,PREVPRICE" +
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

function forceRecalculation() {
    const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
    const sheets = spreadsheet.getSheets();
    let totalFixed = 0;

    // 1. Собираем все ячейки, требующие обновления
    const cellsToUpdate = [];

    sheets.forEach(sheet => {
        if (sheet.isSheetHidden()) return;

        const range = sheet.getDataRange();
        const formulas = range.getFormulas();
        const values = range.getDisplayValues();

        for (let row = 0; row < formulas.length; row++) {
            for (let col = 0; col < formulas[row].length; col++) {
                const displayedValue = values[row][col];
                const formula = formulas[row][col];

                if (formula && displayedValue.startsWith("#") && (formula.includes("getMoex") || formula.includes("getCrypto"))) {
                    const cell = range.getCell(row + 1, col + 1);
                    cellsToUpdate.push({
                        cell: cell,
                        formula: formula,
                    });
                    totalFixed++;
                }
            }
        }
    });

    if (cellsToUpdate.length === 0) {
        SpreadsheetApp.getUi().alert("ℹ️ Нет ячеек, требующих пересчета");
        return;
    }

    // 2. Массово устанавливаем временные значения
    cellsToUpdate.forEach(item => {
        item.cell.setValue("⌛ Обновление...");
    });
    SpreadsheetApp.flush(); // Принудительно применяем все изменения

    // 3. Возвращаем оригинальные формулы
    cellsToUpdate.forEach(item => {
        item.cell.setFormula(item.formula);
    });
    SpreadsheetApp.flush(); // Принудительно применяем все изменения
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
    clearCache(cryptoTicker, cryptoBoardId);
    const cryptoPrice = getCryptoPriceUsd(cryptoTicker);
    const cacheHit = getCryptoPriceUsd(cryptoTicker);
    return result;
}
