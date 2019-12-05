/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

/*
 * lib/buckets.js: bucket-related client API functions.  These functions are
 * invoked by same-named methods in lib/client.js to do the bulk of the work
 * associated with making RPC requests.  The arguments and semantics of these
 * functions are documented in the Boray API.
 */

var stream = require('stream');

var assert = require('assert-plus');

var rpc = require('./rpc');


///--- API

function createBucket(rpcctx, owner, bucket, vnode, req_id, callback) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var arg = {
        owner: owner,
        name: bucket,
        vnode: vnode,
        request_id: req_id
    };

    var log = rpc.childLogger(rpcctx, opts);

    /*
     * electric-boray sends trailing null values with this response.  These are
     * not normally allowed unless we specify ignoreNullValues.
     */
    rpc.rpcCommonSingleMessage({
        rpcctx: rpcctx,
        rpcmethod: 'createbucket',
        rpcargs: [arg],
        ignoreNullValues: true,
        log: log
    }, callback);
}

function getBucket(rpcctx, owner, bucket, vnode, req_id, callback) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var arg = {
        owner: owner,
        name: bucket,
        vnode: vnode,
        request_id: req_id
    };

    var log = rpc.childLogger(rpcctx, opts);

    rpc.rpcCommonSingleMessage({
        rpcctx: rpcctx,
        rpcmethod: 'getbucket',
        rpcargs: [arg],
        log: log
    }, callback);
}

function deleteBucket(rpcctx, owner, bucket, vnode, req_id, callback) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var arg = {
        owner: owner,
        name: bucket,
        vnode: vnode,
        request_id: req_id
    };

    var log = rpc.childLogger(rpcctx, opts);

    rpc.rpcCommonSingleMessage({
        rpcctx: rpcctx,
        rpcmethod: 'deletebucket',
        rpcargs: [arg],
        ignoreNullValues: true,
        log: log
    }, callback);
}

function listBuckets(rpcctx, owner, prefix, limit, marker, vnode, req_id) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.optionalString(prefix, 'prefix');
    assert.number(limit, 'limit');
    assert.optionalString(marker, 'marker');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');

    var opts = {};
    opts.req_id = req_id;

    var arg = {
        owner: owner,
        prefix: prefix,
        limit: limit,
        marker: marker,
        vnode: vnode,
        request_id: req_id
    };

    var log = rpc.childLogger(rpcctx, opts);

    var res = new stream.PassThrough({objectMode: true});

    var req = rpc.rpcCommon({
        rpcctx: rpcctx,
        rpcmethod: 'listbuckets',
        rpcargs: [arg],
        log: log
    }, function (err) {
        if (err) {
            res.emit('error', err);
        } else {
            res.emit('end');
        }

        res.emit('_boray_internal_rpc_done');
    });

    req.pipe(res, {end: false});

    return (res);
}

///--- Exports

module.exports = {
    createBucket: createBucket,
    getBucket: getBucket,
    deleteBucket: deleteBucket,
    listBuckets: listBuckets
};
