/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

/*
 * lib/rpc.js: RPC-related utility functions
 */

var stream = require('stream');

var assert = require('assert-plus');
var VError = require('verror');

///--- API

function childLogger(rpcctx, options) {
    return (rpcctx.createLog(options));
}

/*
 * We provide a few helper methods for making RPC calls using a FastClient:
 *
 *     rpcCommonNoData(args, callback)
 *
 *           Makes an RPC using the arguments specified by "args", reads and
 *           buffers data returned by the RPC, waits for the RPC to complete,
 *           and then invokes "callback" as:
 *
 *               callback(err)
 *
 *           where "err" is an error if the RPC fails or if it succeeds but
 *           returns any data (which is not expected for users of this
 *           function).
 *
 *     rpcCommonBufferData(args, callback)
 *
 *           Makes an RPC using the arguments specified by "args", reads and
 *           buffers data returned by the RPC, waits for the RPC to complete,
 *           and then invokes "callback" as:
 *
 *               callback(err, data)
 *
 *           where "err" is an error only if the RPC fails.  If the RPC
 *           succeeds, "data" is an array of data emitted by the RPC call.
 *
 *     rpcCommon(args, callback)
 *
 *           Makes an RPC using the arguments specified by "args", waits for the
 *           RPC to complete, and then invokes "callback" as:
 *
 *               callback(err)
 *
 *           where "err" is an error only if the RPC fails.  Unlike the
 *           interfaces above, this one does NOT read data from the RPC, which
 *           means the caller is responsible for doing that.  Since the RPC
 *           response is a stream, if the caller does not start reading data,
 *           the RPC will never complete.
 *
 *           This is the building block for the other methods, but it's only
 *           useful if you want to handle data emitted by the RPC in a streaming
 *           way.
 *
 * All of these functions take care of unwrapping errors if the Boray client was
 * configured that way.  Named arguments are:
 *
 *     rpcmethod, rpcargs, log,     See FastClient.rpc() method.
 *     ignoreNullValues
 *
 *     rpcctx                       Boray's "rpcctx" handle, a wrapper around
 *                                  a FastClient that includes context related
 *                                  to this Boray client.
 */
function rpcCommon(args, callback) {
    var rpcctx, req, addrs, res, embeddedError;

    assert.object(args, 'args');
    assert.object(args.rpcctx, 'args.rpcctx');
    assert.string(args.rpcmethod, 'args.rpcmethod');
    assert.array(args.rpcargs, 'args.rpcargs');
    assert.object(args.log, 'args.log');
    assert.optionalNumber(args.timeout, 'args.timeout');
    assert.optionalBool(args.ignoreNullValues, 'args.ignoreNullValues');
    assert.optionalBool(args.wrapErrors, 'args.wrapErrors');
    assert.func(callback);

    res = new stream.PassThrough({objectMode: true});

    rpcctx = args.rpcctx;
    req = rpcctx.fastClient().rpc({
        rpcmethod: args.rpcmethod,
        rpcargs: args.rpcargs,
        timeout: args.timeout,
        ignoreNullValues: args.ignoreNullValues,
        log: args.log
    });

    req.once('end', function () {
        if (embeddedError) {
            args.log.error(embeddedError, 'FastMessage embedded error seen');
            callback(embeddedError);
            return;
        }

        callback();
    });

    req.once('error', function (err) {
        if (rpcctx.unwrapErrors()) {
            err = unwrapError(err);
        } else {
            addrs = rpcctx.socketAddrs();
            err = new VError({
                cause: err,
                info: addrs
            }, 'boray client ("%s" to "%s")', addrs.local, addrs.remote);
        }
        callback(err);
    });


    if (args.wrapErrors) {
        /*
         * Manually pipe req -> res.  This is done so each object can be
         * inspected for an embedded FastMessage error.  If an error is seen,
         * it will be passed to the caller as a proper error object.
         */
        req.on('readable', function () {
            var obj;

            while ((obj = req.read()) !== null) {
                // ignore any records after an error is seen
                if (embeddedError) {
                    return;
                }

                var err = rpcWrapEmbeddedError(obj);

                // an error is seen, save it to be emitted at the end
                if (err) {
                    embeddedError = err;
                    return;
                }

                // no error has been seen (yet), pipe the data along
                res.write(obj);
            }
        });
    } else {
        req.pipe(res, {end: false});
    }

    return (res);
}

/*
 * See above.
 */
function rpcCommonNoData(args, callback) {
    assert.func(callback);
    rpcCommonBufferData(args, function (err, data) {
        if (!err && data.length > 0) {
            err = new VError('bad server response: expected 0 data messages, ' +
                'found %d\n', data.length);
        }

        callback(err);
    });
}

/*
 * See above.
 */
function rpcCommonBufferData(args, callback) {
    var req, rpcmethod, log, data;

    assert.func(callback, 'callback');
    req = rpcCommon(args, function (err) {
        if (err) {
            callback(err);
        } else {
            callback(null, data);
        }
    });

    rpcmethod = args.rpcmethod;
    log = args.log;
    data = [];
    req.on('data', function (obj) {
        log.debug({ 'message': obj }, 'rpc ' + rpcmethod + ': received object');
        data.push(obj);
    });

    return (req);
}

/*
 * Like above, but expects a single message only.
 */
function rpcCommonSingleMessage(args, callback) {
    var req, log;

    assert.object(args, 'args');
    assert.ok(args.log, 'args.log');
    assert.func(callback, 'callback');

    log = args.log.child({func: 'rpcCommonSingleMessage'});

    req = rpcCommonBufferData(args, function (err, data) {
        // forward along any RPC error
        if (err) {
            callback(err);
            return;
        }

        assert.array(data, 'data');

        // ensure there is only 1 message seen
        if (data.length !== 1) {
            err = new VError('bad esponse: expected 1 message, found %d',
                data.length);
            log.error(err, 'multiple messages seen');
            callback(err);
            return;
        }

        // no errors
        var obj = data[0];
        callback(null, obj);
    });

    return (req);
}

/*
 * See the "unwrapErrors" constructor argument for the BorayClient.
 */
function unwrapError(err) {
    if (err.name === 'FastRequestError') {
        err = VError.cause(err);
    }

    if (err.name === 'FastServerError') {
        err = VError.cause(err);
    }

    return (err);
}

/*
 * Wrap an error message embedded in a FastMessage as a VError object.  Returns
 * null if no error is found.
 */
function rpcWrapEmbeddedError(msg) {
    assert.object(msg, 'msg');

    var e;

    if (msg.error) {
        assert.string(msg.error.name, 'msg.error.name');
        assert.string(msg.error.message, 'msg.error.message');

        e = new VError(msg.error.message);
        e.name = msg.error.name;
        return (e);
    }

    return (null);
}


///--- Exports

module.exports = {
    childLogger: childLogger,
    rpcCommon: rpcCommon,
    rpcCommonNoData: rpcCommonNoData,
    rpcCommonBufferData: rpcCommonBufferData,
    rpcCommonSingleMessage: rpcCommonSingleMessage
};
