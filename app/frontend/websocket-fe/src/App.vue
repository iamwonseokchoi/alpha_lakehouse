<template>
  <div id="app">
    <h1>GOOGL Stock Price Data</h1>
    <div v-if="stockData">
      <p><strong>Tick Volume:</strong> {{ stockData.tickVolume }}</p>
      <p><strong>Accumulated Volume:</strong> {{ stockData.accumulatedVolume }}</p>
      <p><strong>Opening Price:</strong> {{ stockData.openingPrice }}</p>
      <p><strong>Volume Weighted Average Price:</strong> {{ stockData.volumeWeightedAveragePrice }}</p>
      <p><strong>Opening Tick:</strong> {{ stockData.openingTick }}</p>
      <p><strong>Closing Tick:</strong> {{ stockData.closingTick }}</p>
      <p><strong>Highest Tick:</strong> {{ stockData.highestTick }}</p>
      <p><strong>Lowest Tick:</strong> {{ stockData.lowestTick }}</p>
      <p><strong>Average Trade Size:</strong> {{ stockData.averageTradeSize }}</p>
      <p><strong>Start Timestamp:</strong> {{ new Date(stockData.startTimestamp).toLocaleString() }}</p>
      <p><strong>End Timestamp:</strong> {{ new Date(stockData.endTimestamp).toLocaleString() }}</p>
    </div>
    <div v-else>
      <p>Loading...</p>
    </div>
  </div>
</template>


<script>
export default {
  data() {
    return {
      stockData: null,
      ws: null,
    };
  },
  mounted() {
    this.connectWebSocket();
  },
  beforeUnmount() {
    this.ws.close();
  },
  methods: {
    connectWebSocket() {
      this.ws = new WebSocket("ws://127.0.0.1:8000/stocks");
      this.ws.addEventListener("message", this.updateStockData);
    },
    updateStockData(event) {
      const rawData = event.data;  // Declare rawData here
      const jsonStr = rawData.replace("Polygon.io Data: ", "");
      try {
        const parsedData = JSON.parse(jsonStr);  // Rename this to avoid duplicate variable name
        if (parsedData && parsedData.evenum === "A" && parsedData.sym === "GOOGL") {
          this.stockData = {
            tickVolume: parsedData.v,
            accumulatedVolume: parsedData.av,
            openingPrice: parsedData.op,
            volumeWeightedAveragePrice: parsedData.vw,
            openingTick: parsedData.o,
            closingTick: parsedData.c,
            highestTick: parsedData.h,
            lowestTick: parsedData.l,
            averageTradeSize: parsedData.z,
            startTimestamp: parsedData.s,
            endTimestamp: parsedData.e
          };
        }
      } catch (e) {
        console.error("Failed to parse JSON data: ", e);
      }
    },
  },
};
</script>


<style>
#app {
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
}
</style>
