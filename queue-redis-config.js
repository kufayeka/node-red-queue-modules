module.exports = function(RED) {
    const poolMgr = require('./redis/redis-pool-manager');

    function UNSRedisConfigNode(config) {
        RED.nodes.createNode(this, config);
        this.name     = config.name;
        this.host     = config.host;
        this.port     = parseInt(config.port, 10);
        this.password = this.credentials.password || undefined;
        this.poolId   = this.id;


        this.options = {
            host: this.host,
            port: this.port,
            password: this.password
        };

        this.getClient = () => poolMgr.getConnection(this.poolId, this.options);
        this.closeClient = () => poolMgr.closeConnection(this.poolId);

        this.on('close', (removed, done) => {
            this.closeClient();
            done();
        });
    }

    RED.nodes.registerType('queue-redis-config', UNSRedisConfigNode, {
        credentials: {
            password: { type: "password" }
        }
    });
};
