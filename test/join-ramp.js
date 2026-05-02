import http from "k6/http";
import { check } from "k6";
import exec from "k6/execution";

const base = __ENV.BASE_URL || "http://localhost:8080";
const tenant = __ENV.TENANT || "load";
const event = __ENV.EVENT || "main";
const sessionPrefix = __ENV.SESSION_PREFIX || "ramp";

const startRate = Number(__ENV.START_RATE || 100);
const peakRate = Number(__ENV.PEAK_RATE || 1000);
const rampUp = __ENV.RAMP_UP || "30s";
const hold = __ENV.HOLD || "1m";
const rampDown = __ENV.RAMP_DOWN || "30s";
const preAllocatedVUs = Number(__ENV.PREALLOCATED_VUS || 200);
const maxVUs = Number(__ENV.MAX_VUS || 2000);

export const options = {
    scenarios: {
        join_ramp: {
            executor: "ramping-arrival-rate",
            timeUnit: "1s",
            preAllocatedVUs,
            maxVUs,
            stages: [
                { duration: rampUp, target: startRate },
                { duration: hold, target: peakRate },
                { duration: rampDown, target: 0 },
            ],
        },
    },
    thresholds: {
        dropped_iterations: ["count<1"],
        http_req_failed: ["rate<0.01"],
        "http_req_duration{type:join}": ["p(95)<50", "p(99)<200"],
    },
};

export default function () {
    const sessionID = `${sessionPrefix}-${exec.scenario.iterationInTest}`;
    const res = http.post(
        `${base}/v1/tenants/${tenant}/events/${event}/queue/join`,
        JSON.stringify({ SessionID: sessionID }),
        {
            headers: { "Content-Type": "application/json" },
            tags: { type: "join" },
        },
    );

    check(res, {
        "join returned 200": (r) => r.status === 200,
        "queue enabled": (r) => r.json("QueueEnabled") === true,
    });
}
