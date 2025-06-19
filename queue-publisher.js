const { Queue } = require("bullmq");

module.exports = function (RED) {
    function QueueRedisPublisher(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        // Ambil Redis client dari config‐node
        const redisCfg = RED.nodes.getNode(config.redisConfig);
        if (!redisCfg) {
            node.error("Missing Redis config");
            return;
        }
        const client = redisCfg.getClient();

        let isRedisReady = false;

        // Tangani event Redis untuk status
        client.on('ready', () => {
            isRedisReady = true;
            node.status({fill:'green',shape:'dot',text:'Redis connected'});
        });
        client.on('error', err => {
            isRedisReady = false;
            node.status({fill:'red',shape:'ring',text:'Redis error'});
        });
        client.on('reconnecting', (ms) => {
            isRedisReady = false;
            node.status({fill:'yellow',shape:'ring',text:`Redis reconnecting (${ms}ms)`});
        });
        client.on('close', () => {
            isRedisReady = false;
            node.status({fill:'red',shape:'ring',text:'Redis closed'});
        });

        const attempts = parseInt(config.attempts || "5");
        const backoffType = config.backoffType || "exponential";
        const backoffDelay = parseInt(config.backoffDelay || "1000");
        const removeAge = parseInt(config.removeAge || "3600");
        const removeCount = parseInt(config.removeCount || "50");
        const removeOnFail = config.removeOnFail === "true";

        const queue = new Queue(config.queueName, { connection: client });

        node.on("input", async (msg, send, done) => {
            try {
                if (!isRedisReady || client.status !== "ready") {
                    node.status({ fill: "red", shape: "ring", text: "Redis not ready" });
                    node.error("Redis is not ready, cannot publish job.", msg);
                    done && done(new Error("Redis not ready"));
                    return;
                }

                const data = msg.payload || {};
                const jobId = msg.jobId || config.jobId || undefined;

                node.status({ fill: "yellow", shape: "dot", text: "publishing…" });

                await queue.add("bullmq-job", data, {
                    jobId,
                    removeOnComplete: {
                        age: removeAge,
                        count: removeCount
                    },
                    removeOnFail,
                    attempts,
                    backoff: {
                        type: backoffType,
                        delay: backoffDelay
                    }
                });

                node.status({ fill: "green", shape: "dot", text: "published" });
                send(msg);
                done();
            } catch (err) {
                node.status({ fill: "red", shape: "ring", text: "publish error" });
                node.error("Publish error: " + err.message, msg);
                done && done(err);
            }
        });

        // Cleanup queue on node close
        node.on("close", async (removed, done) => {
            try {
                await queue.close();
            } catch (e) {
                // Ignore error on close
            }
            done();
        });
    }

    RED.nodes.registerType("queue-publisher", QueueRedisPublisher);
};
