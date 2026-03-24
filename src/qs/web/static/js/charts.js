window.qsCharts = (() => {
  function setTableMessage(table, message, colspan) {
    const columnSpan = colspan || Number(table?.dataset?.colspan || 1);
    table.innerHTML = `
      <tbody>
        <tr><td colspan="${columnSpan}" class="loading-cell">${message}</td></tr>
      </tbody>`;
  }

  function setListMessage(list, message) {
    list.innerHTML = `<div class="stack-item loading-item">${message}</div>`;
  }

  function formatPercent(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
      return "-";
    }
    return `${(value * 100).toFixed(2)}%`;
  }

  function formatNumber(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
      return "-";
    }
    return Number(value).toFixed(2);
  }

  function formatReturn(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
      return "-";
    }
    return `${(value * 100).toFixed(2)}%`;
  }

  function returnClass(value) {
    if (value === null || value === undefined || Number.isNaN(value)) {
      return "";
    }
    if (value > 0) {
      return "return-positive";
    }
    if (value < 0) {
      return "return-negative";
    }
    return "return-flat";
  }

  function buildAnnualReturns(series) {
    const annualBySeries = {};
    const years = new Set();

    series.forEach((item) => {
      const points = [...item.data].sort((left, right) => String(left[0]).localeCompare(String(right[0])));
      const annual = {};
      let currentYear = null;
      let firstNavOfYear = null;
      let lastNavOfYear = null;
      let previousYearEndNav = null;

      const flushYear = () => {
        if (currentYear === null || firstNavOfYear === null || lastNavOfYear === null) {
          return;
        }
        const baseNav = previousYearEndNav ?? firstNavOfYear;
        annual[currentYear] = baseNav ? (lastNavOfYear / baseNav) - 1 : null;
        years.add(currentYear);
        previousYearEndNav = lastNavOfYear;
      };

      points.forEach(([tradeDate, nav]) => {
        const year = String(tradeDate).slice(0, 4);
        if (year !== currentYear) {
          flushYear();
          currentYear = year;
          firstNavOfYear = nav;
        }
        lastNavOfYear = nav;
      });

      flushYear();
      annualBySeries[item.name] = annual;
    });

    return {
      years: Array.from(years).sort((left, right) => right.localeCompare(left)),
      annualBySeries,
    };
  }

  function renderMetricsComparisonTable(table, columns) {
    if (!columns.length) {
      setTableMessage(table, "暂无指标数据");
      return;
    }

    const rows = [
      { label: "CAGR", key: "cagr", formatter: formatPercent },
      { label: "AnnReturn", key: "ann_return", formatter: formatPercent },
      { label: "Sharpe", key: "sharpe", formatter: formatNumber },
      { label: "MDD", key: "max_drawdown", formatter: formatPercent },
      { label: "Kelly", key: "kelly_deploy_ratio", formatter: formatNumber },
    ];

    table.innerHTML = `
      <thead>
        <tr>
          <th>指标</th>
          ${columns.map((item) => `<th>${item.label}</th>`).join("")}
        </tr>
      </thead>
      <tbody>
        ${rows.map((row) => `
          <tr>
            <td>${row.label}</td>
            ${columns.map((item) => `<td>${row.formatter(item.metrics?.[row.key])}</td>`).join("")}
          </tr>`).join("")}
      </tbody>`;
  }

  function renderAnnualReturnsTable(table, series, primaryLabel) {
    const preparedSeries = series.map((item, index) => ({
      name: index === 0 ? primaryLabel : item.name,
      data: item.data,
    }));
    const { years, annualBySeries } = buildAnnualReturns(preparedSeries);

    if (!years.length) {
      setTableMessage(table, "暂无年度收益数据", preparedSeries.length + 1);
      return;
    }

    table.innerHTML = `
      <thead>
        <tr>
          <th>年度</th>
          ${preparedSeries.map((item) => `<th>${item.name}</th>`).join("")}
        </tr>
      </thead>
      <tbody>
        ${years.map((year) => `
          <tr>
            <td>${year}</td>
            ${preparedSeries.map((item) => {
              const value = annualBySeries[item.name]?.[year] ?? null;
              return `<td class="${returnClass(value)}">${formatReturn(value)}</td>`;
            }).join("")}
          </tr>`).join("")}
      </tbody>`;
  }

  function lineOption(series) {
    const dates = series.length ? series[0].data.map((item) => item[0]) : [];
    return {
      tooltip: { trigger: "axis" },
      legend: { top: 8 },
      grid: { left: 40, right: 24, top: 48, bottom: 32 },
      xAxis: { type: "category", data: dates, boundaryGap: false },
      yAxis: { type: "value", scale: true },
      series: series.map((item) => ({
        type: "line",
        name: item.name,
        data: item.data.map((row) => row[1]),
        smooth: true,
        showSymbol: false
      }))
    };
  }

  function bindChartResize(element, chart) {
    if (element.__qsChartCleanup) {
      element.__qsChartCleanup();
    }

    const resize = () => chart.resize();
    let observer = null;
    if (typeof ResizeObserver !== "undefined") {
      observer = new ResizeObserver(() => resize());
      observer.observe(element);
    }
    window.addEventListener("resize", resize);
    element.__qsChartCleanup = () => {
      window.removeEventListener("resize", resize);
      observer?.disconnect();
    };
  }

  function renderLineChart(element, series) {
    const existing = echarts.getInstanceByDom(element);
    if (existing) {
      existing.dispose();
    }

    const chart = echarts.init(element);
    chart.setOption(lineOption(series));
    bindChartResize(element, chart);
    requestAnimationFrame(() => chart.resize());
    return chart;
  }

  async function loadStrategyDetail(siteRoot, runId) {
    const chartElement = document.getElementById("equity-chart");
    const chartLoading = document.getElementById("equity-loading");
    const metricsTable = document.getElementById("metrics-table");
    const annualReturnsTable = document.getElementById("annual-returns-table");
    const holdingsTable = document.getElementById("holdings-table");
    const rebalanceList = document.getElementById("rebalance-list");

    chartElement.classList.add("is-hidden");
    chartLoading.textContent = "加载中...";
    chartLoading.classList.remove("is-hidden");
    setTableMessage(metricsTable, "加载中...");
    setTableMessage(annualReturnsTable, "加载中...");
    setTableMessage(holdingsTable, "加载中...", 5);
    setListMessage(rebalanceList, "加载中...");

    try {
      const [metricsRes, equityRes, benchRes, holdingsRes, rebalanceRes] = await Promise.all([
        fetch(`${window.location.origin}${siteRoot}/api/runs/${runId}/metrics-compare`),
        fetch(`${window.location.origin}${siteRoot}/api/runs/${runId}/equity`),
        fetch(`${window.location.origin}${siteRoot}/api/runs/${runId}/benchmarks`),
        fetch(`${window.location.origin}${siteRoot}/api/runs/${runId}/holdings`),
        fetch(`${window.location.origin}${siteRoot}/api/runs/${runId}/rebalances`)
      ]);
      const responses = [metricsRes, equityRes, benchRes, holdingsRes, rebalanceRes];
      if (responses.some((response) => !response.ok)) {
        throw new Error("加载失败");
      }

      const [metricsData, equity, benchmarks, holdings, rebalances] = await Promise.all([
        metricsRes.json(),
        equityRes.json(),
        benchRes.json(),
        holdingsRes.json(),
        rebalanceRes.json()
      ]);
      renderMetricsComparisonTable(metricsTable, metricsData.columns || []);

      const grouped = {};
      benchmarks.forEach((row) => {
        grouped[row.benchmark_code] = grouped[row.benchmark_code] || [];
        grouped[row.benchmark_code].push([row.trade_date, row.nav]);
      });
      const series = [{name: "Strategy", data: equity.map((row) => [row.trade_date, row.nav])}];
      Object.entries(grouped).forEach(([name, data]) => series.push({name, data}));
      chartLoading.classList.add("is-hidden");
      chartElement.classList.remove("is-hidden");
      renderLineChart(chartElement, series);
      renderAnnualReturnsTable(annualReturnsTable, series, "Strategy");

      if (holdings.length) {
        holdingsTable.innerHTML = `
          <thead><tr><th>代码</th><th>名称</th><th>市场</th><th>原始权重</th><th>Kelly 权重</th></tr></thead>
          <tbody>
            ${holdings.map((row) => `
              <tr>
                <td>${row.symbol}</td>
                <td>${row.symbol_name}</td>
                <td>${row.market}</td>
                <td>${(row.raw_weight * 100).toFixed(2)}%</td>
                <td>${(row.kelly_weight * 100).toFixed(2)}%</td>
              </tr>`).join("")}
          </tbody>`;
      } else {
        setTableMessage(holdingsTable, "暂无持仓数据", 5);
      }

      if (rebalances.length) {
        rebalanceList.innerHTML = rebalances.slice(0, 12).map((row) => `
          <div class="stack-item">
            <strong>${row.rebalance_date}</strong>
            <div>Signal: ${row.signal_date || "-"}</div>
            <div>Targets: ${(row.targets_json || []).join(", ")}</div>
          </div>`).join("");
      } else {
        setListMessage(rebalanceList, "暂无调仓记录");
      }
    } catch (error) {
      chartLoading.textContent = "加载失败";
      chartLoading.classList.remove("is-hidden");
      chartElement.classList.add("is-hidden");
      setTableMessage(metricsTable, "加载失败");
      setTableMessage(annualReturnsTable, "加载失败");
      setTableMessage(holdingsTable, "加载失败", 5);
      setListMessage(rebalanceList, "加载失败");
    }
  }

  function renderComposerResult(data) {
    document.getElementById("composer-metrics").innerHTML = `
      <span>CAGR ${((data.metrics.cagr || 0) * 100).toFixed(2)}%</span>
      <span>Sharpe ${(data.metrics.sharpe || 0).toFixed(2)}</span>
      <span>MDD ${((data.metrics.max_drawdown || 0) * 100).toFixed(2)}%</span>`;

    const grouped = {};
    data.benchmarks.forEach((row) => {
      grouped[row.benchmark_code] = grouped[row.benchmark_code] || [];
      grouped[row.benchmark_code].push([row.trade_date, row.nav]);
    });
    const series = [{name: "Combo", data: data.equity_curve.map((row) => [row.trade_date, row.nav])}];
    Object.entries(grouped).forEach(([name, points]) => series.push({name, data: points}));
    renderLineChart(document.getElementById("composer-chart"), series);
    renderAnnualReturnsTable(document.getElementById("composer-annual-returns"), series, "Combo");

    document.getElementById("composer-weights").innerHTML = `
      <thead><tr><th>策略</th><th>Raw</th><th>Kelly</th></tr></thead>
      <tbody>${data.component_weights.map((row) => `
        <tr><td>${row.strategy_key}</td><td>${(row.raw_weight * 100).toFixed(2)}%</td><td>${(row.kelly_weight * 100).toFixed(2)}%</td></tr>
      `).join("")}</tbody>`;

    document.getElementById("composer-holdings").innerHTML = `
      <thead><tr><th>证券</th><th>名称</th><th>权重</th></tr></thead>
      <tbody>${data.holdings.slice(0, 20).map((row) => `
        <tr><td>${row.symbol}</td><td>${row.symbol_name}</td><td>${(row.kelly_weight * 100).toFixed(2)}%</td></tr>
      `).join("")}</tbody>`;
  }

  return { loadStrategyDetail, renderComposerResult };
})();
