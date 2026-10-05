function airflowAuth(bru) {
  return "Basic " + btoa(bru.getEnvVar("airflow_user") + ":" + bru.getEnvVar("airflow_password"));
}

function limits(bru) {
  return {
    timeoutMs: Number(bru.getEnvVar("poll_timeout_seconds") || 240) * 1000,
    intervalMs: Number(bru.getEnvVar("poll_interval_ms") || 2000)
  };
}

async function waitForDagRun(bru, dagId, dagRunId) {
  if (!dagRunId) {
    throw new Error("no dag_run_id captured: send the Trigger request of this folder first");
  }
  const { timeoutMs, intervalMs } = limits(bru);
  const url = bru.getEnvVar("airflow_url") + "/api/v1/dags/" + dagId + "/dagRuns/" + dagRunId;
  const started = Date.now();
  let state = null;
  let polls = 0;
  while (true) {
    const r = await bru.sendRequest({
      method: "GET",
      url: url,
      headers: { Authorization: airflowAuth(bru), Accept: "application/json" }
    });
    polls += 1;
    state = r.data && r.data.state;
    if (state === "success" || state === "failed") {
      break;
    }
    if (Date.now() - started + intervalMs > timeoutMs) {
      break;
    }
    await bru.sleep(intervalMs);
  }
  return { state: state, polls: polls, waitedMs: Date.now() - started };
}

async function waitForConnector(bru, name, wantedState) {
  if (!name) {
    throw new Error("no connector name captured: send the list connectors request of this folder first");
  }
  const { timeoutMs, intervalMs } = limits(bru);
  const url = bru.getEnvVar("connect_url") + "/connectors/" + name + "/status";
  const started = Date.now();
  let state = null;
  while (true) {
    const r = await bru.sendRequest({ method: "GET", url: url, headers: { Accept: "application/json" } });
    const tasks = (r.data && r.data.tasks) || [];
    state = r.data && r.data.connector && r.data.connector.state;
    if (state === wantedState && tasks.every(function (t) { return t.state === wantedState; })) {
      break;
    }
    if (Date.now() - started + intervalMs > timeoutMs) {
      break;
    }
    await bru.sleep(Math.min(intervalMs, 1000));
  }
  return { state: state, waitedMs: Date.now() - started };
}

module.exports = { waitForDagRun: waitForDagRun, waitForConnector: waitForConnector };
