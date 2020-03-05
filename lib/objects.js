/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * lib/objects.js: object-related client API functions.  These functions are
 * invoked by same-named methods in lib/client.js to do the bulk of the work
 * associated with making RPC requests.  The arguments and semantics of these
 * functions are documented in the Manta buckets metadata API.
 */

var stream = require('stream');

var assert = require('assert-plus');

var rpc = require('./rpc');


///--- API

function createObject(rpcctx, owner, bucket_id, name, object_id, content_length,
    content_md5, content_type, headers, sharks, props, vnode, precondition,
    req_id, callback) {

    var log;

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
    assert.optionalObject(precondition, 'precondition');
    assert.string(req_id, 'req_id');

    var opts = {};
    opts.req_id = req_id;

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
                properties: props,
                precondition: precondition,
                request_id: req_id
              };
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonSingleMessage({
        rpcctx: rpcctx,
        log: log,
        rpcmethod: 'createobject',
        wrapErrors: true,
        rpcargs: [arg]
    }, callback);
}

function getObject(rpcctx, owner, bucket_id, name, vnode, precondition, req_id,
    callback) {

    var log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.optionalObject(precondition, 'precondition');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                vnode: vnode,
                precondition: precondition,
                request_id: req_id
              };

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonSingleMessage({
        rpcctx: rpcctx,
        log: log,
        rpcmethod: 'getobject',
        wrapErrors: true,
        rpcargs: [arg]
    }, callback);
}

function updateObject(rpcctx, owner, bucket_id, name, object_id, content_type,
    headers, props, vnode, precondition, req_id, callback) {

    var log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.string(object_id, 'object_id');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.number(vnode, 'vnode');
    assert.func(callback, 'callback');
    assert.optionalObject(props, 'props');
    assert.optionalObject(precondition, 'precondition');
    assert.string(req_id, 'req_id');

    var opts = {};
    opts.req_id = req_id;

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                id: object_id,
                vnode: vnode,
                content_type: content_type,
                headers: headers,
                properties: props,
                precondition: precondition,
                request_id: req_id
              };
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonSingleMessage({
        rpcctx: rpcctx,
        log: log,
        rpcmethod: 'updateobject',
        wrapErrors: true,
        rpcargs: [arg]
    }, callback);
}

function deleteObject(rpcctx, owner, bucket_id, name, vnode, precondition,
    req_id, callback) {

    var log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.optionalObject(precondition, 'precondition');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                vnode: vnode,
                precondition: precondition,
                request_id: req_id

              };

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonSingleMessage({
        rpcctx: rpcctx,
        log: log,
        rpcmethod: 'deleteobject',
        wrapErrors: true,
        rpcargs: [arg]
    }, callback);
}

function listObjects(rpcctx, owner, bucket_id, prefix, limit, marker, vnode,
    req_id) {

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.optionalString(prefix, 'prefix');
    assert.number(limit, 'limit');
    assert.optionalString(marker, 'marker');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');

    var arg = {
        owner: owner,
        bucket_id: bucket_id,
        prefix: prefix,
        limit: limit,
        marker: marker,
        vnode: vnode,
        request_id: req_id
    };

    var opts = {};
    opts.req_id = req_id;

    var log = rpc.childLogger(rpcctx, opts);

    var res = new stream.PassThrough({objectMode: true});

    var req = rpc.rpcCommon({
        rpcctx: rpcctx,
        rpcmethod: 'listobjects',
        rpcargs: [arg],
        wrapErrors: true,
        log: log
    }, function (err) {
        if (err) {
            res.emit('error', err);
        } else {
            res.emit('end');
        }

        res.emit('_buckets_mdapi_internal_rpc_done');
    });

    req.pipe(res, {end: false});

    return (res);
}

///--- Exports

module.exports = {
    createObject: createObject,
    updateObject: updateObject,
    getObject: getObject,
    deleteObject: deleteObject,
    listObjects: listObjects
};
