const config = require('./config.json');

const event_emitter = require('events');
class processor extends event_emitter {}

const storage_clickhouse = require('./lib/index')

/**
 * {
 *   "clickhouse": {
 *     "host": "127.0.0.1",
 *     "port": 8123,
 *     "user": "default",
 *     "password": "pwd"
 *   }
 * }
 */
let ch = new storage_clickhouse(config.clickhouse);

ch.processor = new processor();

ch.processor.on('error', err => {
    console.log('clickhouse error: ', err);
});

ch.processor.on('log', (data) => {
    console.log('clickhouse log: ', data);
});

/**
 * CREATE TABLE logs (
 * "k" String,
 * "v" String
 * ) ENGINE=MergeTree()
 * ORDER By "k"
 */

for (let i = 0; i < 100000; i++) {
    ch.insert('logs', {k:i, v:'string'+i});
}
