import http from "k6/http";
import { check } from "k6";
import exec from "k6/execution";

const base = __ENV.BASE_URL || "http://localhost:8080";
const tenant = __ENV.TENANT || "load";
const event = __ENV.EVENT || "main";
const sessionPrefix = __ENV.SESSION_PREFIX || "mix";

const rate = Number(__ENV.RATE || 1000);
const duration = __ENV.DURATION || "5m";
const preAllocatedVUs = Number(__ENV.PREALLOCATED_VUS || 400);
const maxVUs = Number(__ENV.MAX_VUS || 2000);

export const options = {
    scenarios: {
        join_status_mix: {
            executor: "constant-arrival-rate",
            rate,
            timeUnit: "1s",
            duration,
            preAllocatedVUs,
            maxVUs,
        },
    },
    thresholds: {
        dropped_iterations: ["count<1"],
        http_req_failed: ["rate<0.01"],
        "http_req_duration{type:join}": ["p(95)<50", "p(99)<200"],
        "http_req_duration{type:status}": ["p(95)<50", "p(99)<200"],
    },
};

export default function () {
    const sessionID = `${sessionPrefix}-${exec.scenario.iterationInTest}`;
    const joinRes = http.post(
        `${base}/v1/tenants/${tenant}/events/${event}/queue/join`,
        JSON.stringify({ SessionID: sessionID }),
        {
            headers: { "Content-Type": "application/json" },
            tags: { type: "join" },
        },
    );

    check(joinRes, {
        "join returned 200": (r) => r.status === 200,
        "queue enabled": (r) => r.json("QueueEnabled") === true,
    });

    const statusRes = http.get(
        `${base}/v1/tenants/${tenant}/events/${event}/queue/status/${sessionID}`,
        {
            tags: { type: "status" },
        },
    );

    check(statusRes, {
        "status returned 200": (r) => r.status === 200,
        "status includes ahead": (r) => r.json("Ahead") !== undefined,
    });
}
