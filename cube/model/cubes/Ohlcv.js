cube(`Ohlcv`, {
  sql: `SELECT * FROM iceberg.silver.ohlcv`,

  measures: {
    count: {
      type: `count`,
    },
    totalVolume: {
      sql: `volume`,
      type: `sum`,
    },
    avgClose: {
      sql: `close`,
      type: `avg`,
    },
    maxClose: {
      sql: `close`,
      type: `max`,
    },
    minClose: {
      sql: `close`,
      type: `min`,
    },
  },

  dimensions: {
    symbol: {
      sql: `symbol`,
      type: `string`,
    },
    time: {
      sql: `time`,
      type: `time`,
    },
    close: {
      sql: `close`,
      type: `number`,
    },
  },
});
