module.exports = function(RED) {
    function QueueFailNode(config) {
        RED.nodes.createNode(this, config);
        const node = this;

        node.on('input', async (msg, send, done) => {
            try {
                const job = msg.__bull_job;
                const reason = msg.failReason || "Manually failed by queue-fail node";
                if (!job) {
                    node.status({ fill: "red", shape: "ring", text: "No BullMQ job in msg" });
                    node.error("No BullMQ job found in msg.__bull_job", msg);
                    return done && done(new Error("No BullMQ job found in msg.__bull_job"));
                }

                await job.moveToFailed(new Error(reason), true); // true: ignore retries

                node.status({ fill: "red", shape: "dot", text: "job failed" });
                msg.failed = true;
                msg.failReason = reason;
                send(msg);
                done();
            } catch (err) {
                node.status({ fill: "red", shape: "ring", text: "fail error" });
                node.error("Queue-fail error: " + err.message, msg);
                done && done(err);
            }
        });
    }

    RED.nodes.registerType("queue-fail", QueueFailNode);
};
