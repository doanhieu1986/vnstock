cube(`Symbols`, {
  sql: `SELECT * FROM iceberg.silver.symbols`,

  measures: {
    count: {
      type: `count`,
    },
  },

  dimensions: {
    symbol: {
      sql: `symbol`,
      type: `string`,
    },
    organName: {
      sql: `organ_name`,
      type: `string`,
    },
  },
});
