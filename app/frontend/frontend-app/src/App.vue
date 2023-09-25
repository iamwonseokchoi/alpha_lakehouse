<template>
  <div id="app">
    <h1>Most Recent Price Data</h1>
    <select v-model="selectedCompany">
      <option v-for="(symbol, name) in companies" :key="symbol" :value="symbol">
        {{ name }}
      </option>
    </select>
    <button @click="fetchData">Fetch Data</button>
    <table v-if="tableData.length" class="styled-table">
      <thead>
        <tr>
          <th>Timestamp</th>
          <th>Close</th>
          <th>High</th>
          <th>Low</th>
          <th>Open</th>
          <th>Transactions</th>
          <th>Volume</th>
          <th>Volume Weighted</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in tableData" :key="row.timestamp">
          <td>{{ row.timestamp }}</td>
          <td>{{ row.close }}</td>
          <td>{{ row.high }}</td>
          <td>{{ row.low }}</td>
          <td>{{ row.open }}</td>
          <td>{{ row.transactions }}</td>
          <td>{{ row.volume }}</td>
          <td>{{ row.volume_weighted }}</td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<script>
import axios from "axios";

export default {
  data() {
    return {
      selectedCompany: null,
      companies: {
        Apple: "aapl",
        Amazon: "amzn",
        Google: "googl",
        Microsoft: "msft",
        Nvidia: "nvda",
        Tesla: "tsla",
      },
      tableData: [],
    };
  },
  methods: {
    async fetchData() {
      if (!this.selectedCompany) return;
      const response = await axios.get(
        `http://localhost:8000/tables/${this.selectedCompany}`
      );
      this.tableData = response.data.data;
    },
  },
};
</script>

<style>
.styled-table {
  width: 100%;
  border-collapse: collapse;
}

.styled-table th,
.styled-table td {
  border: 1px solid #dddddd;
  text-align: left;
  padding: 8px;
}

.styled-table th {
  background-color: #f2f2f2;
}
</style>
