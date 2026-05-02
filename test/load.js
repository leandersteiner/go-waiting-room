import http from "k6/http";
import { check } from "k6";
import exec from "k6/execution";

export const options = {
    scenarios: {
        join: {
            executor: "constant-arrival-rate",
            rate: 10000,
            timeUnit: "1s",
            duration: "5m",
            preAllocatedVUs: 800,
            maxVUs: 2000,
        },
    },
    thresholds: {
        http_req_failed: ["rate<0.01"],
        "http_req_duration{type:join}": ["p(95)<50", "p(99)<200"],
    },
};

const base = __ENV.BASE_URL || "http://localhost:8080";
const tenant = __ENV.TENANT || "load";
const event = __ENV.EVENT || "main";

export default function () {
    const sessionID = `s-${exec.scenario.iterationInTest}`;
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