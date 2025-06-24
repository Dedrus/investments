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
    if (cached) {
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
    if (cached) {
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
    let lastPrice = parseMoexColumn(json.marketdata, moexColumnKeys.lastPrice);
    if (!lastPrice) {
        lastPrice = 0;
    }
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
    if (!lastPrice) {
        lastPrice = 0;
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

    const lockKey = `lock_${ticker}_${boardId}`;
    const cache = getUserCache();

    const lock = cache.get(lockKey);

    if (!lock) {
      try {
        // Устанавливаем блокировку на 30 секунд
        cache.put(lockKey, 'locked', 30);
        
        const url = getMoexShareUrl(ticker, boardId);
        const response = UrlFetchApp.fetch(url);
        const json = JSON.parse(response.getContentText());
        const result = parseMoexShare(json);
        putTickerToCache(ticker, result);
        return result;
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

    const lockKey = `lock_${ticker}_${boardId}`;
    const cache = getUserCache();

    const lock = cache.get(lockKey);

    if (!lock) {
      try {
        // Устанавливаем блокировку на 30 секунд
        cache.put(lockKey, 'locked', 30);
        
        const url = getMoexBondUrl(ticker, boardId);
        const response = UrlFetchApp.fetch(url);
        const json = JSON.parse(response.getContentText());
        const result = parseMoexBond(json);
        putTickerToCache(ticker, result);
        return result;
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
    return JSON.parse(getUserCache().get(ticker));
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

function test() {

    const ticker = "SU26248RMFS3";
    const board = "TQOB";
    getUserCache().remove(ticker);
    const result = getMoexBond(ticker, board);
    const result2 = getMoexBond(ticker, board);
    return result;
}
