// Code powering "developer dashboard" aka devhub, at <https://tigerbeetle.github.io/tigerbeetle>.
//
// At the moment, it isn't clear what's the right style for this kind of non-Zig developer facing
// code, so the following is somewhat arbitrary:
//
// - camelCase naming
// - `deno fmt` for style
// - no TypeScript, no build step

async function main() {
  const runs = await fetchData();
  const series = runsToSeries(runs);
  plotSeries(series, document.querySelector("#charts"));

  const releaseManager = getReleaseManager();
  for (const week of ["previous", "current", "next"]) {
    document.querySelector(`#release-${week}`).textContent =
      releaseManager[week];
  }
}

window.onload = main;

function assert(condition) {
  if (!condition) {
    alert("Assertion failed");
    throw "Assertion failed";
  }
}

function getReleaseManager() {
  const week = getWeek(new Date());
  const candidates = [
    "batiati",
    "cb22",
    "kprotty",
    "matklad",
    "sentientwaffle",
  ];
  candidates.sort();

  return {
    previous: candidates[week % candidates.length],
    current: candidates[(week + 1) % candidates.length],
    next: candidates[(week + 2) % candidates.length],
  };
}

const dataUrl =
  "https://raw.githubusercontent.com/tigerbeetle/devhubdb/main/devhub/data.json";

async function fetchData() {
  const data = await (await fetch(dataUrl)).text();
  return data.split("\n")
    .filter((it) => it.length > 0)
    .map((it) => JSON.parse(it));
}

// The input data is array of runs, where a single run contains many measurements (eg, file size,
// build time).
//
// This function "transposes" the data, such that measurements with identical labels are merged to
// form a single array which is what we want to plot.
//
// This doesn't depend on particular plotting library though.
function runsToSeries(runs) {
  const result = new Map();
  for (const run of runs) {
    for (const measurement of run.measurements) {
      if (!result.has(measurement.label)) {
        result.set(measurement.label, {
          label: measurement.label,
          unit: undefined,
          value: [],
          revision: [],
          timestamp: [],
        });
      }

      const series = result.get(measurement.label);
      assert(series.label == measurement.label);

      if (series.unit) {
        assert(series.unit == measurement.unit);
      } else {
        series.unit = measurement.unit;
      }

      series.value.push(measurement.value);
      series.revision.push(run.revision);
      series.timestamp.push(run.timestamp);
    }
  }
  return result.values();
}

// Plot time series using <https://apexcharts.com>.
function plotSeries(seriesList, rootNode) {
  for (const series of seriesList) {
    let options = {
      title: {
        text: series.label,
      },
      chart: {
        type: "bar",
        height: "400px",
        events: {
          dataPointSelection: (event, chartContext, { dataPointIndex }) => {
            window.open(
              "https://github.com/tigerbeetle/tigerbeetle/commits/" +
                series.revision[dataPointIndex],
            );
          },
        },
      },
      series: [{
        name: series.label,
        data: series.value,
      }],
      xaxis: {
        categories: series.revision.map((sha) => sha.substring(0, 6)),
      },
      tooltip: {
        enabled: true,
        x: {
          formatter: function (val, { dataPointIndex }) {
            const timestamp = new Date(series.timestamp[dataPointIndex] * 1000);
            const formattedDate = timestamp.toISOString();
            return `<div>${val}</div><div>${formattedDate}</div>`;
          },
        },
      },
    };

    if (series.unit === "bytes") {
      options.yaxis = {
        labels: {
          formatter: formatBytes,
        },
      };
    }

    if (series.unit === "ms") {
      options.yaxis = {
        labels: {
          formatter: formatDuration,
        },
      };
    }

    const div = document.createElement("div");
    rootNode.append(div);
    const chart = new ApexCharts(div, options);
    chart.render();
  }
}

function formatBytes(bytes) {
  if (bytes === 0) return "0 Bytes";

  const k = 1024;
  const sizes = [
    "Bytes",
    "KiB",
    "MiB",
    "GiB",
    "TiB",
    "PiB",
    "EiB",
    "ZiB",
    "YiB",
  ];

  let i = 0;
  while (i != sizes.length - 1 && Math.pow(k, i + 1) < bytes) {
    i += 1;
  }

  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

function formatDuration(durationInMilliseconds) {
  const milliseconds = durationInMilliseconds % 1000;
  const seconds = Math.floor((durationInMilliseconds / 1000) % 60);
  const minutes = Math.floor((durationInMilliseconds / (1000 * 60)) % 60);
  const hours = Math.floor((durationInMilliseconds / (1000 * 60 * 60)) % 24);
  const days = Math.floor(durationInMilliseconds / (1000 * 60 * 60 * 24));
  const parts = [];

  if (days > 0) {
    parts.push(`${days}d`);
  }
  if (hours > 0) {
    parts.push(`${hours}h`);
  }
  if (minutes > 0) {
    parts.push(`${minutes}m`);
  }
  if (seconds > 0 || parts.length === 0) {
    parts.push(`${seconds}s`);
  }
  if (milliseconds > 0) {
    parts.push(`${milliseconds}ms`);
  }

  return parts.join(" ");
}

// Returns the ISO week of the date.
//
// Source: https://weeknumber.com/how-to/javascript
function getWeek(date) {
  date = new Date(date.getTime());
  date.setHours(0, 0, 0, 0);
  // Thursday in current week decides the year.
  date.setDate(date.getDate() + 3 - (date.getDay() + 6) % 7);
  // January 4 is always in week 1.
  const week1 = new Date(date.getFullYear(), 0, 4);
  // Adjust to Thursday in week 1 and count number of weeks from date to week1.
  return 1 + Math.round(
    ((date.getTime() - week1.getTime()) / 86400000 -
      3 + (week1.getDay() + 6) % 7) / 7,
  );
}
