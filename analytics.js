
/*
===========================================
 MARKETPULSE - MARKET INTELLIGENCE ENGINE
===========================================

1. Price Movement Detection
2. Volume Spike Detection

Works independently of the Binance connection.
*/

function createMarketAnalytics(config = {}) {

  // ---------------------------------------
  // CONFIGURATION
  // ---------------------------------------

  const CONFIG = {
    priceWindowMs: config.priceWindowMs ?? 20000,

    priceThresholdPct: config.priceThresholdPct ?? 0.5,

    volumeBucketMs: config.volumeBucketMs ?? 10000,

    volumeMultiplier: config.volumeMultiplier ?? 2.5,

    volumeStdDevMultiplier: config.volumeStdDevMultiplier ?? 3,

    minVolumeBaselineBuckets:
      config.minVolumeBaselineBuckets ?? 12,

    cooldownMs: config.cooldownMs ?? 20000
  };

  // ---------------------------------------
  // ASSET-SPECIFIC ANALYTICS STATE
  // ---------------------------------------

  const assets = {};

  function getAssetState(symbol) {

    if (!assets[symbol]) {

      assets[symbol] = {

        priceHistory: [],

        lastPriceAlertAt: 0,

        currentBucketStart: null,

        currentBucketVolume: 0,

        completedVolumes: [],

        lastVolumeAlertAt: 0

      };

    }

    return assets[symbol];
  }

  // ---------------------------------------
  // ALERT GENERATOR
  // ---------------------------------------

  function createAlert({
    type,
    symbol,
    message,
    severity,
    observedValue,
    baselineValue,
    changePct
  }) {

    return {

      id: `${symbol}-${type}-${Date.now()}`,

      type,

      symbol,

      message,

      severity,

      observedValue,

      baselineValue,

      changePct,

      timestamp: new Date()

    };
  }

  // ---------------------------------------
  // 1. PRICE MOVEMENT DETECTION
  // ---------------------------------------

  function detectPriceMovement(event, state, now) {

    const alerts = [];

    const currentPrice = event.price;

    const cutoff = now - CONFIG.priceWindowMs;

    // Find the latest available price at or before
    // the beginning of the detection window.

    let reference = null;

    for (let i = state.priceHistory.length - 1; i >= 0; i--) {

      if (state.priceHistory[i].time <= cutoff) {

        reference = state.priceHistory[i];

        break;

      }

    }

    // Store current price.

    state.priceHistory.push({

      time: now,

      price: currentPrice

    });

    // Keep memory usage bounded.

    state.priceHistory = state.priceHistory.filter(

      point => point.time >= now - CONFIG.priceWindowMs * 2

    );

    if (!reference) return alerts;

    const percentageChange =

      ((currentPrice - reference.price) / reference.price) * 100;

    const absoluteChange = Math.abs(percentageChange);

    if (absoluteChange < CONFIG.priceThresholdPct) {

      return alerts;

    }

    // Prevent repeated alerts for the same sustained movement.

    if (now - state.lastPriceAlertAt < CONFIG.cooldownMs) {

      return alerts;

    }

    state.lastPriceAlertAt = now;

    const direction = percentageChange >= 0
      ? "increased"
      : "decreased";

    const severity = absoluteChange >= CONFIG.priceThresholdPct * 2
      ? "HIGH"
      : "MEDIUM";

    alerts.push(

      createAlert({

        type: "PRICE_MOVEMENT",

        symbol: event.symbol,

        message:
          `${event.symbol} price ${direction} by ` +
          `${percentageChange.toFixed(4)}% ` +
          `within ${CONFIG.priceWindowMs / 1000} seconds`,

        severity,

        observedValue: currentPrice,

        baselineValue: reference.price,

        changePct: Number(percentageChange.toFixed(4))

      })

    );

    return alerts;

  }

  // ---------------------------------------
  // 2. VOLUME SPIKE DETECTION
  // ---------------------------------------

  function finalizeVolumeBucket(symbol, state, bucketVolume, bucketTime) {

    const alerts = [];

    // Use only PREVIOUS completed buckets as the baseline.

    const baseline = state.completedVolumes.slice(

      -CONFIG.minVolumeBaselineBuckets

    );

    if (baseline.length >= CONFIG.minVolumeBaselineBuckets) {

      const mean = baseline.reduce(

        (sum, value) => sum + value,
        0

      ) / baseline.length;

      const variance = baseline.reduce(

        (sum, value) => sum + Math.pow(value - mean, 2),
        0

      ) / baseline.length;

      const standardDeviation = Math.sqrt(variance);

      const threshold = Math.max(

        mean * CONFIG.volumeMultiplier,

        mean +
        CONFIG.volumeStdDevMultiplier * standardDeviation

      );

      if (bucketVolume > threshold && bucketVolume > 0) {

        const ratio = mean > 0
          ? bucketVolume / mean
          : null;

        const severity = ratio !== null && ratio >= 4
          ? "HIGH"
          : "MEDIUM";

        alerts.push(

          createAlert({

            type: "VOLUME_SPIKE",

            symbol,

            message:
              `${symbol} trading volume spike detected. ` +
              `Volume: ${bucketVolume.toFixed(8)}, ` +
              `Baseline: ${mean.toFixed(8)}`,

            severity,

            observedValue: Number(bucketVolume.toFixed(10)),

            baselineValue: Number(mean.toFixed(10)),

            changePct: ratio !== null
              ? Number(((ratio - 1) * 100).toFixed(2))
              : null

          })

        );

      }

    }

    // Add the completed bucket to historical baseline.

    state.completedVolumes.push(bucketVolume);

    // Keep only the latest 30 completed buckets.

    state.completedVolumes = state.completedVolumes.slice(-30);

    return alerts;

  }

  function detectVolumeSpike(event, state, now) {

    const alerts = [];

    const bucketStart =

      Math.floor(now / CONFIG.volumeBucketMs) *
      CONFIG.volumeBucketMs;

    // Initialize the first bucket.

    if (state.currentBucketStart === null) {

      state.currentBucketStart = bucketStart;

    }

    // A new bucket has started.
    // Finalize the previous interval.

    if (bucketStart > state.currentBucketStart) {

      alerts.push(

        ...finalizeVolumeBucket(

          event.symbol,

          state,

          state.currentBucketVolume,

          state.currentBucketStart

        )

      );

      // Fill missing intervals with zero-volume buckets.
      // This keeps the baseline time-based.

      let missingStart =

        state.currentBucketStart + CONFIG.volumeBucketMs;

      let missingCount = 0;

      while (
        missingStart < bucketStart &&
        missingCount < 30
      ) {

        state.completedVolumes.push(0);

        state.completedVolumes =
          state.completedVolumes.slice(-30);

        missingStart += CONFIG.volumeBucketMs;

        missingCount++;

      }

      // If there was a long interruption, discard the
      // stale baseline and warm up again.

      if (missingStart < bucketStart) {

        state.completedVolumes = [];

      }

      state.currentBucketStart = bucketStart;

      state.currentBucketVolume = 0;

    }

    // Accumulate quantity in the current interval.

    state.currentBucketVolume += event.quantity;

    // Volume is evaluated when the next interval begins.

    return alerts;

  }

  // ---------------------------------------
  // MAIN ANALYTICS ENTRY POINT
  // ---------------------------------------

  function processTrade(event) {

    if (
      !event ||
      !event.symbol ||
      !Number.isFinite(event.price) ||
      !Number.isFinite(event.quantity) ||
      event.price <= 0 ||
      event.quantity <= 0
    ) {

      return [];

    }

    const now = Date.now();

    const state = getAssetState(event.symbol);

    const alerts = [];

    // Run both independent detection systems.

    alerts.push(

      ...detectPriceMovement(event, state, now)

    );

    alerts.push(

      ...detectVolumeSpike(event, state, now)

    );

    return alerts;

  }

  // ---------------------------------------
  // PUBLIC API
  // ---------------------------------------

  return {

    processTrade,

    getConfig: () => ({ ...CONFIG })

  };

}

module.exports = createMarketAnalytics;
