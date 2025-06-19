const { Worker } = require('bullmq');

module.exports = function(RED) {
    function QueueRedisConsumer(config) {
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

        node.queueName = config.queueName || "default";
        node.workerName = config.workerName || "worker";
        node.concurrency = parseInt(config.concurrency || "1");
        node.delayMs = parseInt(config.delayMs || "0");

        const worker = new Worker(node.queueName, async job => {
            node.status({ fill: "yellow", shape: "dot", text: "processing…" });

            // delay processing
            if (node.delayMs > 0) {
                await new Promise(resolve => setTimeout(resolve, node.delayMs));
            }

            node.send({
                payload: job.data,
                jobId: job.id,
                jobName: job.name,
                timestamp: job.timestamp,
                processedOn: job.processedOn,
                finishedOn: job.finishedOn,
                queueName: node.queueName,
                __bull_job: job // untuk advanced usage
            });

            node.status({ fill: "green", shape: "dot", text: "waiting…" });
        }, {
            connection: client,
            concurrency: node.concurrency
        });

        worker.on("completed", job => {
            node.status({ fill: "green", shape: "dot", text: "completed" });
        });

        worker.on("failed", (job, err) => {
            node.status({ fill: "red", shape: "ring", text: "failed" });
            node.error("Job failed: " + err.message, { jobId: job.id, data: job.data });
        });

        worker.on("error", err => {
            node.status({ fill: "red", shape: "ring", text: "worker error" });
            node.error("Worker error: " + err.message);
        });

        node.on("close", () => {
            worker.close().then(r => {
                node.status({ fill: "grey", shape: "ring", text: "worker closed" });
            });
        });
    }

    RED.nodes.registerType("queue-consumer", QueueRedisConsumer);
}
