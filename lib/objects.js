/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019, Joyent, Inc.
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
    content_md5, content_type, headers, sharks, props, vnode, req_id,
    callback) {
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
                request_id: req_id
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
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var args = [ owner,
                 bucket_id,
                 name,
                 object_id,
                 content_length,
                 content_md5,
                 content_type,
                 headers,
                 sharks,
                 props,
                 req_id
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


function getObject(rpcctx, owner, bucket_id, name, vnode, req_id, callback) {
    var log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                vnode: vnode,
                request_id: req_id
              };

    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonSingleMessage({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'getobject',
        'rpcargs': [arg]
    }, callback);
}


function getObjectNoVnode(rpcctx, owner, bucket_id, name, req_id, callback) {
    var log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var args = [ owner,
                 bucket_id,
                 name,
                 req_id
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


function updateObject(rpcctx, owner, bucket_id, name, object_id, content_type,
    headers, props, vnode, req_id, callback) {
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
                request_id: req_id
              };
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'updateobject',
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

function updateObjectNoVnode(rpcctx, owner, bucket_id, name, object_id,
    content_type, headers, props, req_id, callback) {
    var log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.string(object_id, 'object_id');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var args = [ owner,
                 bucket_id,
                 name,
                 object_id,
                 content_type,
                 headers,
                 props,
                 req_id
              ];
    log = rpc.childLogger(rpcctx, opts);
    rpc.rpcCommonBufferData({
        'rpcctx': rpcctx,
        'log': log,
        'rpcmethod': 'updateobject',
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


function deleteObject(rpcctx, owner, bucket_id, name, vnode, req_id, callback) {
    var log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var arg = { owner: owner,
                bucket_id: bucket_id,
                name: name,
                vnode: vnode,
                request_id: req_id
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

function deleteObjectNoVnode(rpcctx, owner, bucket_id, name, req_id, callback) {
    var log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.string(req_id, 'req_id');
    assert.func(callback, 'callback');

    var opts = {};
    opts.req_id = req_id;

    var args = [ owner,
                 bucket_id,
                 name,
                 req_id
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

        if (data.length === 1) {
            callback(null, data[0]);
        } else {
            callback(new VError('expected 1 data messages, found %d',
                data.length));
        }
    });
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

function listObjectsNoVnode(rpcctx, owner, bucket_id, prefix, limit, marker,
    delimiter, req_id) {

    var log;

    assert.object(rpcctx, 'rpcctx');
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.optionalString(prefix, 'prefix');
    assert.number(limit, 'limit');
    assert.optionalString(marker, 'marker');
    assert.optionalString(delimiter, 'delimiter');
    assert.string(req_id, 'req_id');

    var opts = {};
    opts.req_id = req_id;

    log = rpc.childLogger(rpcctx, opts);

    var res = new EventEmitter();

    var arg = [
        owner,
        bucket_id,
        prefix,
        limit,
        marker,
        delimiter,
        req_id
    ];

    var req = rpc.rpcCommon({
        rpcctx: rpcctx,
        rpcmethod: 'listobjects',
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
    createObject: createObject,
    createObjectNoVnode: createObjectNoVnode,
    updateObject: updateObject,
    updateObjectNoVnode: updateObjectNoVnode,
    getObject: getObject,
    getObjectNoVnode: getObjectNoVnode,
    deleteObject: deleteObject,
    deleteObjectNoVnode: deleteObjectNoVnode,
    listObjects: listObjects,
    listObjectsNoVnode: listObjectsNoVnode
};
