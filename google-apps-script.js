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
    lCurrentPrice: "LCURRENTPRICE" // цена
};

function getMoexShareLastPrice(ticker, boardId) {
    const cached = getCachedTicker(ticker);
    if (cached && cached.lastPrice) {
        return cached.lastPrice;
    }
    const result = getAndCacheMoexShareData(ticker, boardId);
    return result.lastPrice;
}

function getMoexBondField(ticker, boardId, fieldName) {
    return getMoexBond(ticker, boardId)[fieldName];
}

function getMoexShareShortName(ticker, boardId) {
    const cached = getCachedTicker(ticker);
    if (cached && cached.shortName) {
        return cached.shortName;
    }
    const result = getAndCacheMoexShareData(ticker, boardId);
    return result.shortName;
}

function getMoexBond(ticker, boardId) {
    const cached = getCachedTicker(ticker);
    if (cached) {
        return cached;
    }
    return getAndCacheMoexBondData(ticker, boardId);
}

function parseMoexShare(json) {
    const lastPrice = parseMoexColumn(json.marketdata, moexColumnKeys.lastPrice);
    const shortName = parseMoexColumn(json.securities, moexColumnKeys.shortName);

    return {
        lastPrice,
        shortName,
    };
}

function getCryptoPriceUsd(ticker) {
    const cached = getCachedTicker(ticker);
    if (cached) {
        return cached.lastPrice;
    }
    const lockKey = `lock_${ticker}_crypto`;
    const cache = getUserCache();

    const lock = cache.get(lockKey);

    if (!lock) {
      try {
        // Устанавливаем блокировку на 30 секунд
        cache.put(lockKey, 'locked', 30);
        const url = "https://cryptoprices.cc/" + ticker;
        const response = UrlFetchApp.fetch(url);
        const result = response.getContentText().trim();
        putTickerToCache(ticker, { lastPrice: result });
        return result;
      } finally {
        // Снимаем блокировку
        cache.remove(lockKey);
      }
    } else {
      // Ждем и рекурсивно пробуем снова
      Utilities.sleep(1000 + Math.random() * 2000);
      return getCryptoPriceUsd(ticker);
    }

}

function parseMoexBond(json) {
    let lastPrice = parseMoexColumn(json.marketdata, moexColumnKeys.lCurrentPrice);
    if (!lastPrice) {
        lastPrice = parseMoexColumn(json.marketdata, moexColumnKeys.lastPrice);
    }

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

function getAndCacheMoexShareData(ticker, boardId) {
    if (boardId === undefined || boardId === null) {
        boardId = "TQBR";
    }
    const cached = getCachedTicker(ticker);
    if (cached) {
        return cached;
    }
    const lockKey = `lock_${ticker}_${boardId}`;
    const cache = getUserCache();

    const lock = cache.get(lockKey);

    if (!lock) {
      try {
        // Устанавливаем блокировку на 30 секунд
        cache.put(lockKey, 'locked', 30);
        
        const url = getMoexShareUrl(ticker, boardId);
        let attempt = 0;
        while (attempt < 6) {
          try {
            const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
            if (response.getResponseCode() === 200) {
                  const json = JSON.parse(response.getContentText());
                  const result = parseMoexShare(json);
                  putTickerToCache(ticker, result);
                  return result;
              }
          } catch (e) {
              console.warn(`Attempt ${attempt + 1} failed:`, ticker, boardId, e);
          }
        sleep(1000 * (attempt + 1)); // Экспоненциальное увеличение задержки
        attempt++;
        }
      } finally {
        // Снимаем блокировку
        cache.remove(lockKey);
      }
    } else {
      // Ждем и рекурсивно пробуем снова
      sleep(1000 + getRandomInt(100, 500));
      return getAndCacheMoexShareData(ticker, boardId);
    }
    
}

function getAndCacheMoexBondData(ticker, boardId) {
    if (boardId === undefined || boardId === null) {
        throw new Error("Provide board id");
    }
    const cached = getCachedTicker(ticker);
    if (cached) {
        return cached;
    }

    const lockKey = `lock_${ticker}_${boardId}`;
    const cache = getUserCache();

    const lock = cache.get(lockKey);

    if (!lock) {
      try {
        // Устанавливаем блокировку на 30 секунд
        cache.put(lockKey, 'locked', 30);
        
        const url = getMoexBondUrl(ticker, boardId);
        let attempt = 0;
        while (attempt < 6) {
          try {
            const response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
            if (response.getResponseCode() === 200) {
                  const json = JSON.parse(response.getContentText());
                  const result = parseMoexBond(json);
                  putTickerToCache(ticker, result);
                  return result;
              }
          } catch (e) {
              console.warn(`Attempt ${attempt + 1} failed:`, ticker, boardId, e);
          }
        sleep(1000 * (attempt + 1)); // Экспоненциальное увеличение задержки
        attempt++;
        }
      } finally {
        // Снимаем блокировку
        cache.remove(lockKey);
      }
    } else {
      // Ждем и рекурсивно пробуем снова
      sleep(1000 + getRandomInt(100, 500));
      return getAndCacheMoexBondData(ticker, boardId);
    }
}

function getCachedTicker(ticker) {
  const cached = getUserCache().get(ticker);
  return cached ? JSON.parse(cached) : null;
}

function putTickerToCache(ticker, result) {
    getUserCache().put(ticker, JSON.stringify(result), 60 * 30);
}

function getUserCache() {
    return CacheService.getUserCache();
}

function getMoexShareUrl(ticker, boardId) {
    return "https://iss.moex.com/iss/engines/stock/markets/shares/boards/" + boardId + "/securities/" + ticker + ".json?iss.meta=off&iss.only=marketdata,securities&marketdata.columns=LAST&securities.columns=SHORTNAME";
}

function getMoexBondUrl(ticker, boardId) {
    return "https://iss.moex.com/iss/engines/stock/markets/bonds/boards/" + boardId + "/securities/" + ticker + ".json?iss.meta=off&iss.only=marketdata,securities,marketdata_yields" +
      "&marketdata.columns=LAST,DURATION,YIELDTOOFFER,LCURRENTPRICE&securities.columns=SHORTNAME,SECNAME,LOTVALUE,COUPONVALUE,NEXTCOUPON,ACCRUEDINT,MATDATE,COUPONPERIOD,BUYBACKPRICE,COUPONPERCENT,OFFERDATE&marketdata_yields.columns=EFFECTIVEYIELD";
}

function sleep(milliseconds) {
    Utilities.sleep(milliseconds);
}

function getRandomInt(min, max) {
    const minCeiled = Math.ceil(min);
    const maxFloored = Math.floor(max);
    return Math.floor(Math.random() * (maxFloored - minCeiled) + minCeiled); // The maximum is exclusive and the minimum is inclusive
}

function clearCache(ticker) {
    CacheService.getUserCache().remove(ticker);
}

/**
 * Функция создает меню в Google Таблице при открытии.
 * onOpen() — специальное имя, Google вызывает его автоматически.
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();

  ui.createMenu('📈 Инвестиции')
    .addItem('Пересчитать ошибки', 'forceRecalculation')
    .addToUi();
}

/**
 * Функция для пересчета упавших ячеек
 */
function forceRecalculation() {
  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = spreadsheet.getSheets();
  let totalFixed = 0;
  const startTime = new Date();

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
        // Проверка времени выполнения (чтобы не превысить лимит)
        if (new Date() - startTime > 25000) {
          SpreadsheetApp.getUi().alert(`⚠️ Прервано: перезапущено ${totalFixed} ячеек`);
          return;
        }
        
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
    getUserCache().remove(ticker);
    const result = getMoexBond(ticker, board);
    const result2 = getMoexBond(ticker, board);

    const shareTicker = "SBER";
    getUserCache().remove(shareTicker);
    const shareName = getMoexShareShortName(shareTicker);
    const sharePrice = getMoexShareLastPrice(shareTicker);
    return result;
}
