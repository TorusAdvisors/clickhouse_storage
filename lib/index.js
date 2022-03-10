const http = require('http');
const fs = require('fs');

/** https://clickhouse.com/docs/ru/interfaces/http/ **/

module.exports = class storage_clickhouse {
    constructor(params) {
        this.params = params;

        this.buffer = new Map();
        this.errors = new Map();
        this.timer = null;
        this.timer_miss = 0;
        this.current_timer_interval = 1000;
        this.timer_default_interval = 1000;
        this.max_values_per_query = 1000;

        this.processor = null;

        this.line_recieved = 0;
        this.line_inserted = 0;
    }

    http_post(query, post, cb) {
        let o = this;

        let options = {
            'method': 'POST',
            'hostname': o.params.host,
            'port': o.params.port,
            'path': '/?query='+encodeURIComponent(query),
            'headers': {
                'Authorization': 'Basic '+Buffer.from(o.params.user + ':' + o.params.password).toString('base64'),
                'Content-Type': 'text/plain',
                'X-ClickHouse-Progress': 1
            },
        };

        let req = http.request(options, function (res) {
            let chunks = [];

            res.on("data", function (chunk) {
                chunks.push(chunk);
            });

            res.on("end", function (chunk) {
                let body = Buffer.concat(chunks);
                if (cb !== undefined) {
                    if (res.statusCode !== 200) {
                        cb(body.toString(), null);
                    } else {
                        cb(null, body.toString());
                    }
                }
            });

            res.on("error", function (error) {
                cb(error, null);
            });
        });

        req.on('error', function (error) {
            if (cb !== undefined) {
                cb(error, null)
            }
        });

        req.on('socket', function (socket) {
            socket.setTimeout(10000);
            socket.on('timeout', function() {
                req.abort();
            });
        });

        if (post) {
            req.write(post);
        }
        req.end();
    }

    debug(...args) {
        if (process.env.CLICKHOUSE_STORAGE_DEBUG) {
            this.processor.emit('log', JSON.stringify([...args]));
            console.log(...args);
        }
    }

    clearTimer(full) {
        full = full || false;
        let o = this;
        o.debug('clear timer');
        if (full) {
            o.current_timer_interval = o.timer_default_interval;
        }

        o.line_inserted = 0;
        o.line_recieved = 0;

        clearInterval(o.timer);
        o.timer = null;
    }

    runTimer() {
        let o = this;

        o.timer = setInterval(() => {

            o.debug('inserted lines: ', o.line_inserted);
            o.debug('received lines: ', o.line_recieved);

            if (o.timer_miss >= 10) {
                o.clearTimer(true);
            }

            let buffer_is_not_empty = false;
            o.debug('timer -> check buffer');
            o.buffer.forEach((value, key, map) => {
                if (value.values && value.values.length > 0) {
                    o.debug('buffer: '+key+' num:'+value.values.length);
                    buffer_is_not_empty = true;
                    o.debug('run flush_buffer from timer');
                    o.flush_buffer(key);
                }
            });

            if (buffer_is_not_empty === false) {
                o.debug('buffer is empty: '+o.timer_miss);
                o.timer_miss++;
                return;
            }

            o.debug('clear timer miss');
            o.timer_miss = 0;
        }, o.current_timer_interval);
    }

    flush_buffer(buffer_key) {
        let o = this;

        let buffer = o.buffer.get(buffer_key);

        let buffer_for_insert = Object.assign(buffer);

        let values_for_insert = [];
        if (buffer_for_insert.values && buffer_for_insert.values.length > (o.max_values_per_query+100)) {
            // run timer optimization
            if (buffer_for_insert.values.length > o.max_values_per_query*5
                && o.current_timer_interval === o.timer_default_interval
            ) {
                console.log('run storage optimizer');
                o.current_timer_interval = 50;
                o.clearTimer();
                o.runTimer();
                return;
            }

            values_for_insert = buffer_for_insert.values.splice(0, o.max_values_per_query);
            buffer.values = buffer_for_insert.values;
            o.buffer.set(buffer_key, buffer);
        } else {
            values_for_insert = buffer_for_insert.values;
            o.buffer.delete(buffer_key);
        }

        if (values_for_insert.length > 0) {
            this.insert_sql(buffer.sql, values_for_insert, buffer.errors_key);
        }
    }

    init(cb) {
        let o = this;

        o.line_recieved = 0;
        o.line_inserted = 0;

        o.http_post("SELECT 'test ok';", null, (err, result) => {
            if (!err && result.indexOf('test ok') !== -1) {
                cb();
                return;
            }
            console.log(err);
        })
    }

    insert(table_name, row, errors_key) {
        let o = this;

        o.line_recieved++;

        //o.debug('insert', table_name, row);

        let sql = 'INSERT INTO '+table_name+' (';
        let sql_keys = [];
        Object.keys(row).forEach((row_key, key_index) => {
            sql_keys.push('"'+row_key+'"');
        });

        sql += sql_keys.join(', ')+') ';
        sql += 'FORMAT CSV ';

        let buffer_key = table_name+'_'+errors_key+'_'+sql_keys.join('_');

        let buffer = o.buffer.get(buffer_key);
        if (!buffer) {
            buffer = {sql:sql, errors_key:errors_key, values:[]};
            o.buffer.set(buffer_key, buffer);
        }

        //o.debug('add to buffer');
        buffer.values.push(Object.values(row));

        /**
        if (buffer.values.length > 300 && buffer.values.length < 1000) {
            o.debug('exec flush_buffer from insert method');
            o.flush_buffer(buffer_key);
        }*/

        if (o.timer === null) {
            o.debug('run timer from insert method');
            o.runTimer();
        }
    }

    insert_sql(query, values, errors_key) {
        errors_key = errors_key || 'all';
        let o = this;

        let data = [];
        values.forEach((item) => {
            let line = '';
            let elements = [];
            item.forEach((e) => {
                if (typeof e === 'string') {
                    elements.push('"'+e.replace(/"/g, '""')+'"');
                } else {
                    elements.push('"'+e+'"');
                }
            });
            line += elements.join(',');
            data.push(line);
        });

        o.debug('insert', query);
        o.http_post(query, data.join("\n"), (err, result) => {
            if (err) {
                o.debug('insert error:', err);
                o.processor.emit('error', JSON.stringify({err:err, query:query, values:values}));
                let errors_container = o.errors.get(errors_key);
                if (!errors_container) {
                    errors_container = [];
                    o.errors.set(errors_key, errors_container);
                }

                errors_container.push(err);
                return;
            }

            let num_lines = data.length;
            o.line_inserted += num_lines;

            o.debug('inserted: ', num_lines);
            o.processor.emit('inserted: ', num_lines);
        });
    }

    query(query, cb) {
        let o = this;

        o.http_post(query, null, (err, res) => {
            cb(err, res);
        });
    }

    close() {

    }
}