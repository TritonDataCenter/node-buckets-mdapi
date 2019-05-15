/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * lib/objects.js: object-related client API functions.  These functions are
 * invoked by same-named methods in lib/client.js to do the bulk of the work
 * associated with making RPC requests.  The arguments and semantics of these
 * functions are documented in the Boray API.
 */

var EventEmitter = require('events').EventEmitter;
var stream = require('stream');

var assert = require('assert-plus');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var VError = require('verror');

var rpc = require('./rpc');


///--- API

function createObject(rpcctx, owner, bucket_id, name, object_id, content_length,
    content_md5, content_type, headers, sharks, props, vnode, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.string(object_id, 'object_id');
    assert.number(content_length, 'content_length');
    assert.string(content_md5, 'content_md5');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.object(sharks, 'sharks');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');
    assert.optionalObject(props, 'props');

    opts = makeOptions({});

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                id: object_id,
                vnode: vnode,
                content_length: content_length,
                content_md5: content_md5,
                content_type: content_type,
                headers: headers,
                sharks: sharks,
                properties: props
              };
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'createobject',
        'rpcargs': [arg]
    }, function (err, data) {
        if (!err && data.length > 1) {
            err = new VError('expected at most 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data.length === 0 ? {} : data[0]);
        }
    });
}

function createObjectNoVnode(rpcctx, owner, bucket_id, name, object_id,
    content_length, content_md5, content_type, headers, sharks, props,
    callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.string(object_id, 'object_id');
    assert.number(content_length, 'content_length');
    assert.string(content_md5, 'content_md5');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.object(sharks, 'sharks');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var args = [ owner,
                 bucket_id,
                 name,
                 object_id,
                 content_length,
                 content_md5,
                 content_type,
                 headers,
                 sharks,
                 props
              ];
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'createobject',
        'rpcargs': args
    }, function (err, data) {
        if (!err && data.length > 1) {
            err = new VError('expected at most 1 data message, found %d',
                data.length);
        }

        if (err) {
            callback(err);
        } else {
            callback(null, data.length === 0 ? {} : data[0]);
        }
    });
}


function getObject(rpcctx, owner, bucket_id, name, vnode, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                vnode: vnode
              };

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'getobject',
        'rpcargs': [arg]
    }, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        if (data.length === 1) {
            callback(null, data[0]);
        } else {
            callback(new VError('expected 1 data messages, found %d',
                data.length));
        }
    });
}

function getObjectNoVnode(rpcctx, owner, bucket_id, name, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var args = [ owner,
                 bucket_id,
                 name
               ];

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'getobject',
        'rpcargs': args
    }, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        if (data.length === 1) {
            callback(null, data[0]);
        } else {
            callback(new VError('expected 1 data messages, found %d',
                data.length));
        }
    });
}


function deleteObject(rpcctx, owner, bucket_id, name, vnode, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                vnode: vnode
              };

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'deleteobject',
        'rpcargs': [arg]
    }, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        if (data.length === 1) {
            callback(null, data[0]);
        } else {
            callback(new VError('expected 1 data messages, found %d',
                data.length));
        }
    });
}

function deleteObjectNoVnode(rpcctx, owner, bucket_id, name, callback) {
    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.func(callback, 'callback');

    opts = makeOptions({});

    var args = [ owner,
                 bucket_id,
                 name
               ];

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'deleteobject',
        'rpcargs': args
    }, function (err, data) {
        if (err) {
            callback(err);
            return;
        }

        callback(null, data);
    });
}


function listObjects(rpcctx, owner, bucket_id, order_by, prefix, limit, offset,
    vnode) {

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(order_by, 'order_by');
    assert.string(prefix, 'prefix');
    assert.number(limit, 'limit');
    assert.number(offset, 'offset');
    assert.number(vnode, 'vnode');

    var arg = {
        owner: owner,
        bucket_id: bucket_id,
        order_by: order_by,
        prefix: prefix,
        limit: limit,
        offset: offset,
        vnode: vnode
    };

    var opts = makeOptions(arg);

    var log = rpc.childLogger(rpcctx, opts);

    var res = new stream.PassThrough({objectMode: true});

    var req = rpc.rpcCommon({
        rpcctx: rpcctx,
        rpcmethod: 'listobjects',
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

function listObjectsNoVnode(rpcctx, owner, bucket_id, sorted, order_by, prefix,
    limit) {

    var opts, log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.bool(sorted, 'sorted');
    assert.string(order_by, 'order_by');
    assert.string(prefix, 'prefix');
    assert.number(limit, 'limit');

    opts = makeOptions({});

    log = rpc.childLogger(rpcctx, opts);

    var res = new EventEmitter();

    var req = rpc.rpcCommon({
        rpcctx: rpcctx,
        rpcmethod: 'listobjects',
        rpcargs: [owner, bucket_id, sorted, order_by, prefix, limit],
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


///--- Helpers

function makeOptions(options, value) {
    var opts = jsprim.deepCopy(options);

    // Defaults handlers
    opts.req_id = options.req_id || libuuid.create();

    return (opts);
}


///--- Exports

module.exports = {
    createObject: createObject,
    getObject: getObject,
    deleteObject: deleteObject,
    createObjectNoVnode: createObjectNoVnode,
    getObjectNoVnode: getObjectNoVnode,
    deleteObjectNoVnode: deleteObjectNoVnode,
    listObjects: listObjects,
    listObjectsNoVnode: listObjectsNoVnode
};
