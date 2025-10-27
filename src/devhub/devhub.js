// Code powering "developer dashboard" aka devhub, at <https://tigerbeetle.github.io/tigerbeetle>.
//
// At the moment, it isn't clear what's the right style for this kind of non-Zig developer facing
// code, so the following is somewhat arbitrary:
//
// - snake_case naming
// - `deno fmt` for style
// - no TypeScript, no build step

window.onload = () =>
  Promise.all([
    main_release_rotation(),
    main_seeds(),
    main_metrics(),
  ]);

function assert(condition) {
  if (!condition) {
    alert("Assertion failed");
    throw new Error("Assertion failed");
  }
}

function main_release_rotation() {
  const release_manager = get_release_manager();
  for (const week of ["previous", "current", "next"]) {
    document.querySelector(`#release-${week}`).textContent =
      release_manager[week];
  }

  function get_release_manager() {
    const shift = 1; // Adjust when changing the set of candidates to avoid shifts.
    const week = get_week(new Date()) + shift;
    const candidates = [
      "batiati",
      "cb22",
      "chaitanyabhandari",
      "fabioarnold",
      "lewisdaly",
      "matklad",
      "sentientwaffle",
      "toziegler",
      "GeorgKreuzmayr",
    ];
    candidates.sort();

    return {
      previous: candidates[week % candidates.length],
      current: candidates[(week + 1) % candidates.length],
      next: candidates[(week + 2) % candidates.length],
    };
  }
}

async function main_seeds() {
  const data_url =
    "https://raw.githubusercontent.com/tigerbeetle/devhubdb/main/fuzzing/data.json";
  const issues_url =
    "https://api.github.com/repos/tigerbeetle/tigerbeetle/issues?per_page=200";
  const logs_base =
    "https://raw.githubusercontent.com/tigerbeetle/devhubdb/main/";

  const [records, issues] = await Promise.all([
    fetch_json(data_url),
    fetch_json(issues_url),
  ]);

  const pulls = issues.filter((issue) => issue.pull_request);
  const pulls_by_url = new Map(
    pulls.map((pull) => [pull.pull_request.html_url, pull]),
  );
  const open_pull_requests = new Set(pulls.map((it) => it.number));
  const untriaged_issues = issues.filter((issue) =>
    !issue.pull_request &&
    !issue.labels.map((label) => label.name).includes("triaged")
  );
  document.querySelector("#untriaged-issues-count").innerText =
    untriaged_issues.length;
  if (untriaged_issues.length) {
    document.querySelector("#untriaged-issues-count").classList.add(
      "untriaged",
    );
  }

  // Filtering:
  // - By default, show one seed per fuzzer per commit; exclude successes for the main branch and
  //   already merged pull requests.
  // - Clicking on the fuzzer cell in the table shows all seeds for this fuzzer/commit pair.
  // - "show all" link (in the .html) disables filtering completely.
  const query = new URLSearchParams(document.location.search);
  const query_fuzzer = query.get("fuzzer");
  const query_commit = query.get("commit");
  const query_all = query.get("all") !== null;
  const fuzzers_with_failures = new Set();

  const table_dom = document.querySelector("#seeds>tbody");
  let commit_previous = undefined;
  let seed_fail_count = 0;
  let vpm = undefined; // VOPRs per minute

  for (const record of records) {
    if (
      !vpm && record.fuzzer === "vopr" && is_main(record) && record.count > 100
    ) {
      const elapsed_seconds = Date.now() / 1000 - record.seed_timestamp_start;
      vpm = (record.count * 60) / elapsed_seconds;
    }

    let include = undefined;
    if (query_all) {
      include = true;
    } else if (query_fuzzer || query_commit) {
      include = (!query_fuzzer || record.fuzzer == query_fuzzer) &&
        (!query_commit || record.commit_sha == query_commit);
    } else if (
      pull_request_number(record) &&
      !open_pull_requests.has(pull_request_number(record))
    ) {
      include = false;
    } else if (record.fuzzer === "canary" && !is_main(record)) {
      include = false;
    } else if (record.fuzzer === "vopr" && is_release(record)) {
      include = true;
    } else {
      include = (!record.ok || pull_request_number(record) !== undefined) &&
        !fuzzers_with_failures.has(record.branch + record.fuzzer);
      if (include) fuzzers_with_failures.add(record.branch + record.fuzzer);
    }

    if (!include) continue;

    const seed_duration_ms =
      (record.seed_timestamp_end - record.seed_timestamp_start) * 1000;
    const seed_freshness_ms = Date.now() - (record.seed_timestamp_start * 1000);
    const staleness_threshold_ms = 3 * 60 * 60 * 1000;
    const canery_is_stale = record.fuzzer === "canary" &&
      !pull_request_number(record) &&
      seed_freshness_ms > staleness_threshold_ms;
    const staleness_warning = canery_is_stale
      ? '<span title="Canary check is older than 3 hours.">⚠️</span>'
      : "";

    const row_dom = document.createElement("tr");

    let seed_success = record.fuzzer === "canary" ? !record.ok : record.ok;
    if (canery_is_stale) seed_success = false;
    if (seed_success) {
      row_dom.classList.add("success");
    } else {
      seed_fail_count++;
    }
    if (record.commit_sha != commit_previous) {
      commit_previous = record.commit_sha;
      row_dom.classList.add("group-start");
    }

    const pull = pulls_by_url.get(record.branch);
    let commit_extra = "(unknown)";
    if (pull_request_number(record)) {
      commit_extra = `<a href="${record.branch}">#${
        pull_request_number(record)
      }</a>`;
    } else if (is_main(record)) {
      commit_extra = "(main)";
    } else if (is_release(record)) {
      commit_extra = "(release)";
    }
    const log_link = record.log ? ` <a href="${logs_base + record.log}">(log)</a>` : "";
    row_dom.innerHTML = `
          <td>
            <a href="https://github.com/tigerbeetle/tigerbeetle/commit/${record.commit_sha}"><code>${
      record.commit_sha.substring(0, 7)
    }</code></a>
            ${commit_extra}
          </td>
          <td>${pull ? pull.user.login : ""}</td>
          <td><a href="?fuzzer=${record.fuzzer}&commit=${record.commit_sha}">${record.fuzzer}</a></td>
          <td><code onclick="copy_to_clipboard(this)">${record.command}</code></td>
          <td><time>${format_duration(seed_duration_ms)}</time>${log_link}</td>
          <td>
            <time>${format_duration(seed_freshness_ms)} ago</time>
            ${staleness_warning}
          </td>
          <td>
            ${record.count.toLocaleString("en-US").replace(/,/g, "&nbsp;")}
          </td>
      `;
    table_dom.appendChild(row_dom);
  }

  if (vpm) {
    document.querySelector("#vpm").innerHTML = `${Math.floor(vpm)} VPM`;
  }

  let main_branch_fail = 0;
  let main_branch_ok = 0;
  let main_branch_canary = 0;
  for (const record of records) {
    if (is_main(record)) {
      if (record.fuzzer === "canary") {
        main_branch_canary += record.count;
      } else if (record.ok) {
        main_branch_ok += record.count;
      } else {
        main_branch_fail += record.count;
      }
    }
  }
  if (main_branch_fail > 0 && !query_commit && !query_fuzzer) {
    // When there are failures on main and we don't query for a specific commit/fuzzer,
    // there should be failing seeds in our table.
    assert(seed_fail_count > 0);
  }
}

async function main_metrics() {
  const data_url =
    "https://raw.githubusercontent.com/tigerbeetle/devhubdb/main/devhub/data.json";
  const data = await (await fetch(data_url)).text();
  const max_batches = 200;
  const batches = data.split("\n")
    .filter((it) => it.length > 0)
    .map((it) => JSON.parse(it))
    .slice(-1 * max_batches)
    .reverse();

  const series = batches_to_series(batches);
  plot_series(series, document.querySelector("#charts"), batches.length);
}

function is_main(record) {
  return record.branch === "https://github.com/tigerbeetle/tigerbeetle";
}

function is_release(record) {
  return record.branch ===
    "https://github.com/tigerbeetle/tigerbeetle/tree/release";
}

function pull_request_number(record) {
  const pr_prefix = "https://github.com/tigerbeetle/tigerbeetle/pull/";
  if (record.branch.startsWith(pr_prefix)) {
    const pr_number = record.branch.substring(
      pr_prefix.length,
      record.branch.length,
    );
    return parseInt(pr_number, 10);
  }
  return undefined;
}

function format_duration(duration_ms) {
  const milliseconds = duration_ms % 1000;
  const seconds = Math.floor((duration_ms / 1000) % 60);
  const minutes = Math.floor((duration_ms / (1000 * 60)) % 60);
  const hours = Math.floor((duration_ms / (1000 * 60 * 60)) % 24);
  const days = Math.floor(duration_ms / (1000 * 60 * 60 * 24));
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
  if (days == 0) {
    if (seconds > 0 || parts.length === 0) {
      parts.push(`${seconds}s`);
    }
    if (hours == 0 && minutes == 0) {
      if (milliseconds > 0) {
        parts.push(`${milliseconds}ms`);
      }
    }
  }

  return parts.join(" ");
}

// Returns the ISO week of the date.
//
// Source: https://weeknumber.com/how-to/javascript
function get_week(date) {
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

// The input data is array of runs, where a single run contains many measurements (eg, file size,
// build time).
//
// This function "transposes" the data, such that measurements with identical labels are merged to
// form a single array which is what we want to plot.
//
// This doesn't depend on particular plotting library though.
function batches_to_series(batches) {
  const results = new Map();
  for (const [index, batch] of batches.entries()) {
    for (const metric of batch.metrics) {
      if (!results.has(metric.name)) {
        results.set(metric.name, {
          name: metric.name,
          unit: undefined,
          value: [],
          git_commit: [],
          timestamp: [],
        });
      }

      const series = results.get(metric.name);
      assert(series.name == metric.name);

      if (series.unit) {
        assert(series.unit == metric.unit);
      } else {
        series.unit = metric.unit;
      }

      // Even though our x-axis is time, we want to spread things out evenly by batch, rather than
      // group according to time. Apex charts is much quicker when given an x value, even though it
      // isn't strictly needed.
      series.value.push([batches.length - index, metric.value]);
      series.git_commit.push(batch.attributes.git_commit);
      series.timestamp.push(batch.timestamp);
    }
  }

  return Array.from(results.values());
}

function plot_series(series_list, root_node, batch_count) {
  const now_seconds = Date.now() / 1000;
  const outlier_count = 3;

  const outile_indices = series_list.map(
    (series, index) => ({
      series,
      index,
      score: outlier_score(series, now_seconds),
    }),
  )
    .sort((a, b) => b.score - a.score)
    .slice(0, outlier_count)
    .map((it) => it.index);

  const series_list_ordered = [
    ...outile_indices.map((index) => series_list[index]),
    ...series_list.filter((_, index) => !outile_indices.includes(index)),
  ];

  for (const [series_index, series] of series_list_ordered.entries()) {
    const options = {
      title: {
        text: series.name,
      },
      chart: {
        id: series.name,
        group: "devhub",
        type: "line",
        height: "400px",
        animations: {
          enabled: false,
        },
        events: {
          dataPointSelection: (event, chartContext, { dataPointIndex }) => {
            window.open(
              "https://github.com/tigerbeetle/tigerbeetle/commit/" +
                series.git_commit[dataPointIndex],
            );
          },
        },
      },
      markers: {
        size: 4,
      },
      colors: series_index < outlier_count ? ["var(--red-10)"] : undefined,
      series: [{
        name: series.name,
        data: series.value,
      }],
      xaxis: {
        categories: Array(series.value[series.value.length - 1][0]).fill("")
          .concat(
            series.timestamp.map((timestamp) =>
              format_date_day(new Date(timestamp * 1000))
            ).reverse(),
          ),
        min: 0,
        max: batch_count,
        tickAmount: 15,
        axisTicks: {
          show: false,
        },
        tooltip: {
          enabled: false,
        },
      },
      tooltip: {
        enabled: true,
        shared: false,
        intersect: true,
        x: {
          formatter: function (val, { dataPointIndex }) {
            const formattedDate = format_date_day_time(
              new Date(series.timestamp[dataPointIndex] * 1000),
            );
            return `<div>${
              series.git_commit[dataPointIndex]
            }</div><div>${formattedDate}</div>`;
          },
        },
      },
    };

    const formatters = {
      bytes: format_bytes,
      ms: format_duration,
      s: (s) => format_duration(s * 1000),
      count: format_count,
    };

    if (formatters[series.unit]) {
      options.yaxis = {
        labels: {
          formatter: formatters[series.unit],
        },
      };
    }

    const div = document.createElement("div");
    root_node.append(div);
    const chart = new ApexCharts(div, options);
    chart.render();
  }
}

// Heuristic function that takes a time series and returns a number
// proportional to week-on-week change.
function outlier_score(series, now_seconds) {
  const WEEK = 7 * 24 * 60 * 60;

  const recent = [];
  const baseline = [];

  for (let i = 0; i < series.value.length; i++) {
    const value = series.value[i][1];
    const age = now_seconds - series.timestamp[i];

    if (age <= WEEK) {
      recent.push(value);
    } else if (age <= 2 * WEEK) {
      baseline.push(value);
    } else {
      // Older than 2 weeks: discarded.
    }
  }

  if (recent.length === 0 || baseline.length === 0) {
    return 0;
  }

  const recent_mean = mean(recent);
  const baseline_mean = mean(baseline);

  if (baseline_mean === 0) return 0;

  return Math.abs(recent_mean - baseline_mean) / baseline_mean;
}

function mean(values) {
  assert(values.length > 0);
  let sum = 0;
  for (const v of values) sum += v;
  return sum / values.length;
}

function format_bytes(bytes) {
  return format_suffix(bytes, 1024, [
    "Bytes",
    "KiB",
    "MiB",
    "GiB",
    "TiB",
    "PiB",
    "EiB",
    "ZiB",
    "YiB",
  ]);
}

function format_count(count) {
  return format_suffix(count, 1000, ["", "K", "M", "G"]);
}

function format_suffix(amount, base, progression) {
  if (amount == 0) return `0 ${progression[0]}`;
  let i = 0;
  while (i != progression.length - 1 && Math.pow(base, i + 1) < amount) {
    i += 1;
  }
  return `${parseFloat((amount / Math.pow(base, i)).toFixed(2))} ${
    progression[i]
  }`;
}

function format_date_day(date) {
  return format_date(date, false);
}

function format_date_day_time(date) {
  return format_date(date, true);
}

function format_date(date, include_time) {
  assert(date instanceof Date);

  const pad = (number) => String(number).padStart(2, "0");

  const year = date.getFullYear();
  const month = pad(date.getMonth() + 1); // Months are 0-based.
  const day = pad(date.getDate());
  const hours = pad(date.getHours());
  const minutes = pad(date.getMinutes());
  const seconds = pad(date.getSeconds());
  return include_time
    ? `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
    : `${year}-${month}-${day}`;
}

async function fetch_json(url) {
  const response = await fetch(url, { cache: "no-cache" });
  return await response.json();
}

function copy_to_clipboard(element) {
  navigator.clipboard.writeText(element.innerText).then(() => {
    const before = element.innerHTML;
    element.innerText = "Copied!";
    setTimeout(() => element.innerHTML = before, 1000);
  });
}
