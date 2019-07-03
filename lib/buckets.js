/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * lib/buckets.js: bucket-related client API functions.  These functions are
 * invoked by same-named methods in lib/client.js to do the bulk of the work
 * associated with making RPC requests.  The arguments and semantics of these
 * functions are documented in the Boray API.
 */

var EventEmitter = require('events').EventEmitter;
var stream = require('stream');

var assert = require('assert-plus');
var libuuid = require('libuuid');
var jsprim = require('jsprim');
var VError = require('verror');

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
    rpc.rpcCommonBufferData({
        rpcctx: rpcctx,
        rpcmethod: 'createbucket',
        rpcargs: [arg],
        ignoreNullValues: true,
        log: log
    }, function (err, buckets) {
        if (err) {
            callback(err);
            return;
        }

        assert.array(buckets, 'buckets');

        if (buckets.length !== 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
            callback(err);
            return;
        }

        callback(null, buckets[0]);
    });
}

function createBucketNoVnode(rpcctx, owner, bucket, req_id, callback) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var log = rpc.childLogger(rpcctx, opts);

    /*
     * electric-boray sends trailing null values with this response.  These are
     * not normally allowed unless we specify ignoreNullValues.
     */
    rpc.rpcCommonBufferData({
        rpcctx: rpcctx,
        rpcmethod: 'createbucket',
        rpcargs: [owner, bucket, req_id],
        ignoreNullValues: true,
        log: log
    }, function (err, buckets) {
        if (err) {
            callback(err);
            return;
        }

        assert.array(buckets, 'buckets');

        if (buckets.length !== 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
            callback(err);
            return;
        }

        callback(null, buckets[0]);
    });
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

    rpc.rpcCommonBufferData({
        rpcctx: rpcctx,
        rpcmethod: 'getbucket',
        rpcargs: [arg],
        log: log
    }, function (err, buckets) {
        if (err) {
            callback(err);
            return;
        }

        assert.array(buckets, 'buckets');

        if (buckets.length !== 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
            callback(err);
            return;
        }

        callback(null, buckets[0]);
    });
}

function getBucketNoVnode(rpcctx, owner, bucket, req_id, callback) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var log = rpc.childLogger(rpcctx, opts);

    rpc.rpcCommonBufferData({
        rpcctx: rpcctx,
        rpcmethod: 'getbucket',
        rpcargs: [owner, bucket, req_id],
        log: log
    }, function (err, buckets) {
        if (err) {
            callback(err);
            return;
        }

        assert.array(buckets, 'buckets');

        if (buckets.length !== 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
            callback(err);
            return;
        }

        callback(null, buckets[0]);
    });
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

    rpc.rpcCommonBufferData({
        rpcctx: rpcctx,
        rpcmethod: 'deletebucket',
        rpcargs: [arg],
        ignoreNullValues: true,
        log: log
    }, function (err, buckets) {
        if (err) {
            callback(err);
            return;
        }

        assert.array(buckets, 'buckets');

        if (buckets.length !== 1) {
            err = new VError('bad server response: expected 1 bucket, found %d',
                buckets.length);
            callback(err);
            return;
        }

        callback(null, buckets[0]);
    });
}

function deleteBucketNoVnode(rpcctx, owner, bucket, req_id, callback) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket, 'bucket');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var log = rpc.childLogger(rpcctx, opts);

    rpc.rpcCommonBufferData({
        rpcctx: rpcctx,
        rpcmethod: 'deletebucket',
        rpcargs: [owner, bucket, req_id],
        ignoreNullValues: true,
        log: log
    }, function (err, buckets) {
        if (err) {
            callback(err);
        } else {
            callback(null, buckets);
        }
    });
}

/*
 * This function talks to boray
 */
function listBuckets(rpcctx, owner, order_by, prefix, limit, offset, vnode,
    req_id) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(order_by, 'order_by');
    assert.string(prefix, 'prefix');
    assert.number(limit, 'limit');
    assert.number(offset, 'offset');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');

    var opts = {};
    opts.req_id = req_id;

    var arg = {
        owner: owner,
        order_by: order_by,
        prefix: prefix,
        limit: limit,
        offset: offset,
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

/*
 * This function talks to electric-boray
 */
function listBucketsNoVnode(rpcctx, owner, sorted, order_by, prefix, limit,
    req_id) {
    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.bool(sorted, 'sorted');
    assert.string(order_by, 'order_by');
    assert.string(prefix, 'prefix');
    assert.number(limit, 'limit');
    assert.string(req_id, 'req_id');

    var opts = {};
    opts.req_id = req_id;

    var arg = [owner, sorted, order_by, prefix, limit, req_id];

    var log = rpc.childLogger(rpcctx, opts);

    var res = new EventEmitter();

    var req = rpc.rpcCommon({
        rpcctx: rpcctx,
        rpcmethod: 'listbuckets',
        rpcargs: arg,
        log: log
    }, function (err) {
        if (err) {
            res.emit('error', err);
        } else {
            res.emit('end');
        }

        res.emit('_boray_internal_rpc_done');
    });

    req.on('data', function (msg) {
        res.emit('record', msg);
    });

    return (res);
}

///--- Exports

module.exports = {
    createBucket: createBucket,
    createBucketNoVnode: createBucketNoVnode,
    getBucket: getBucket,
    getBucketNoVnode: getBucketNoVnode,
    deleteBucket: deleteBucket,
    deleteBucketNoVnode: deleteBucketNoVnode,
    listBuckets: listBuckets,
    listBucketsNoVnode: listBucketsNoVnode
};
