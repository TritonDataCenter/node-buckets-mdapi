/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * lib/client.js: Boray client implementation.  The BorayClient object is the
 * handle through which consumers make RPC requests to a remote Boray server.
 */

var EventEmitter = require('events').EventEmitter;
var path = require('path');
var util = require('util');

var assert = require('assert-plus');
var cueball = require('cueball');
var fast = require('fast');
var jsprim = require('jsprim');
var libuuid = require('libuuid');
var VError = require('verror');

var BorayConnectionPool = require('./pool');
var FastConnection = require('./fast_connection');
var buckets = require('./buckets');
var meta = require('./meta');
var objects = require('./objects');
var parseBorayParameters = require('./client_params').parseBorayParameters;


///--- Default values for function arguments

var fastNRecentRequests         = 5;
var dflClientTcpKeepAliveIdle   = 10000;  /* milliseconds */

/*
 * See BorayClient() constructor.
 */
var BORAY_CS_OPEN    = 'open';
var BORAY_CS_CLOSING = 'closing';


///--- Helpers

function emitUnavailable() {
    var emitter = new EventEmitter();
    setImmediate(function () {
        emitter.emit('error', new Error('no active connections'));
    });
    return (emitter);
}


///--- API

/*
 * Constructor for the boray client.
 *
 * This client uses the cueball module to maintain a pool of TCP connections to
 * the IP addresses associated with a DNS name.  cueball is responsible for
 * DNS resolution (periodically, in the background) and maintaining the
 * appropriate TCP connections, while we maintain a small abstraction for
 * balancing requests across connections.
 *
 * The options accepted, the constraints on them, and several examples are
 * described in the boray(3) manual page inside this repository.  Callers can
 * also specify any number of legacy options documented with
 * populateLegacyOptions().
 */
function BorayClient(options) {
    var self = this;
    var coptions, cueballOptions, resolverInput;
    var resolver;

    EventEmitter.call(this);

    assert.object(options, 'options');
    assert.optionalObject(options.collector, 'options.collector');
    assert.object(options.log, 'options.log');
    assert.optionalBool(options.unwrapErrors, 'options.unwrapErrors');
    assert.optionalBool(options.failFast, 'options.failFast');
    assert.optionalBool(options.requireIndexes, 'options.requireIndexes');
    assert.optionalBool(options.requireOnlineReindexing,
        'options.requireOnlineReindexing');
    assert.optionalNumber(options.crc_mode, 'options.crc_mode');

    coptions = parseBorayParameters(options);
    cueballOptions = coptions.cueballOptions;

    /* Read-only metadata used for toString() and the like. */
    this.hostLabel = coptions.label;

    this.unwrapErrors = options.unwrapErrors ? true : false;
    this.failFast = options.failFast ? true : false;
    this.requireIndexes = options.requireIndexes ? true : false;
    this.requireOnlineReindexing =
        options.requireOnlineReindexing ? true : false;
    this.crc_mode = options.crc_mode || fast.FAST_CHECKSUM_V1;

    /* Helper objects. */
    this.log = options.log.child({
        component: 'BorayClient',
        domain: cueballOptions.domain
    }, true);

    this.log.debug(coptions, 'init');

    /* Optional artedi metrics collector that we'll pass to fast, if set. */
    if (options.collector) {
        this.collector = options.collector;
    }

    if (coptions.mode === 'srv') {
        resolverInput = cueballOptions.domain;
    } else {
        resolverInput = cueballOptions.domain + ':' +
            cueballOptions.defaultPort;
    }

    resolver = cueball.resolverForIpOrDomain({
        'input': resolverInput,
        'resolverConfig': {
            'resolvers': cueballOptions.resolvers,
            'recovery': cueballOptions.recovery,
            'service': cueballOptions.service,
            'defaultPort': cueballOptions.defaultPort,
            'maxDNSConcurrency': cueballOptions.maxDNSConcurrency,
            'log': this.log.child({ 'component': 'CueballResolver' }, true)
        }
    });
    if (resolver instanceof Error) {
        throw new VError(resolver, 'invalid boray client configuration');
    }
    resolver.start();
    this.cueballResolver = resolver;

    this.cueball = new cueball.ConnectionSet({
        'constructor': function cueballConstructor(backend) {
            return (self.createFastConnection(backend));
        },
        'log': this.log.child({ 'component': 'CueballSet' }, true),
        'resolver': resolver,
        'recovery': cueballOptions.recovery,
        'target': cueballOptions.target,
        'maximum': cueballOptions.maximum,
        'connectionHandlesError': true
    });

    /* Internal state. */
    this.nactive = 0;           /* count of outstanding RPCs */
    this.timeConnected = null;  /* time when first cueball conn established */
    this.ncontexts = 0;         /* counter of contexts ever created */
    this.activeContexts = {};   /* active RPC contexts (requests) */
    this.timeCueballInitFailed = null;   /* cueball entered "failed" */

    /*
     * State recorded when close() is invoked.  The closeState is one of:
     *
     *     BORAY_CS_OPEN        close() has never been invoked
     *
     *     BORAY_CS_CLOSING     close() has been invoked, but we have not
     *                          finished closing (presumably because outstanding
     *                          requests have not yet aborted)
     *
     *     BORAY_CS_CLOSED      close process has completed and there are no
     *                          connections in use any more.
     */
    this.closeState = BORAY_CS_OPEN;    /* see above */
    this.nactiveAtClose = null;         /* value of "nactive" at close() */

    /*
     * If requested, add a handler to ensure that the process does not exit
     * without this client being closed.
     */
    if (options.mustCloseBeforeNormalProcessExit) {
        this.onprocexit = function processExitCheck(code) {
            if (code === 0) {
                throw (new Error('process exiting before boray client closed'));
            }
        };
        process.on('exit', this.onprocexit);
    } else {
        this.onprocexit = null;
    }

    this.pool = new BorayConnectionPool({
        'log': this.log,
        'cueballResolver': this.cueballResolver,
        'cueballSet': this.cueball
    });

    this.cueballOnStateChange = function (st) {
        self.onCueballStateChange(st);
    };

    this.cueball.on('stateChanged', this.cueballOnStateChange);
}

util.inherits(BorayClient, EventEmitter);

/*
 * This getter is provided for historical reasons.  It's not a great interface.
 * It's intrinsically racy (i.e., the state may change as soon as the caller has
 * checked it), and it doesn't reflect much about the likelihood of a request
 * succeeding.  It's tempting to simply always report "true" so that clients
 * that might be tempted to avoid making a request when disconnected would just
 * go ahead and try it (which will fail quickly if we are disconnected anyway).
 * But we settle on the compromise position of reporting only whether we've
 * _ever_ had a connection.  This accurately reflects what many clients seem
 * interested in, which is whether we've set up yet.
 */
Object.defineProperty(BorayClient.prototype, 'connected', {
    'get': function () {
        return (this.timeConnected !== null &&
            this.closeState === BORAY_CS_OPEN);
    }
});

/*
 * During startup, we wait for the state of the Cueball ConnectionSet to reach
 * "running", at which point we emit "connect" so that callers know that they
 * can start using this client.
 *
 * If "failFast" was specified in the constructor, then if the ConnectionSet
 * reaches "failed" before "connected", then we emit an error and close the
 * client.  See the "failFast" documentation above for details.
 */
BorayClient.prototype.onCueballStateChange = function onCueballStateChange(st) {
    var err;

    assert.strictEqual(this.timeConnected, null);
    assert.strictEqual(this.timeCueballInitFailed, null);

    this.log.trace({ 'newState': st }, 'cueball state change');

    if (st === 'running') {
        this.timeConnected = new Date();
        this.cueball.removeListener('stateChanged', this.cueballOnStateChange);
        this.log.debug('client ready');
        this.emit('connect');
    } else if (this.failFast && st === 'failed') {
        this.timeCueballInitFailed = new Date();
        this.cueball.removeListener('stateChanged', this.cueballOnStateChange);
        err = new VError('boray client "%s": failed to establish connection',
            this.hostLabel);
        this.log.warn(err);
        this.emit('error', err);
        this.close();
    }
};

/**
 * Aborts outstanding requests, shuts down all connections, and closes this
 * client down.
 */
BorayClient.prototype.close = function close() {
    var self = this;

    if (this.closeState !== BORAY_CS_OPEN) {
        this.log.warn({
            'closeState': this.closeState,
            'nactiveAtClose': this.nactiveAtClose
        }, 'ignoring close() after previous close()');
        return;
    }

    if (this.onprocexit !== null) {
        process.removeListener('exit', this.onprocexit);
        this.onprocexit = null;
    }

    this.closeState = BORAY_CS_CLOSING;
    this.nactiveAtClose = this.nactive;
    this.log.info({ 'nactiveAtClose': this.nactive }, 'closing');
    this.pool.fallbackDisable();

    if (this.nactive === 0) {
        setImmediate(function closeImmediate() { self.closeFini(); });
        return;
    }

    /*
     * Although we would handle sockets destroyed underneath us, the most
     * straightforward way to clean up is to proactively terminate outstanding
     * requests, wait for them to finish, and then stop the set.  We do this by
     * detaching each underlying Fast client from its socket.  This should cause
     * Fast to fail any oustanding requests, causing the RPC contexts to be
     * released, and allowing us to proceed with closing.
     */
    jsprim.forEachKey(this.activeContexts, function (_, rpcctx) {
        rpcctx.fastClient().detach();
    });
};

BorayClient.prototype.closeFini = function closeFini() {
    assert.equal(this.closeState, BORAY_CS_CLOSING);
    assert.equal(this.nactive, 0);
    assert.ok(jsprim.isEmpty(this.activeContexts));

    var self = this;
    this.cueball.on('stateChanged', function (st) {
        if (st === 'stopped') {
            self.log.info('closed');
            self.emit('close');
        }
    });

    this.log.info('waiting for cueball to stop');
    this.cueball.stop();
    this.cueballResolver.stop();
};


BorayClient.prototype.toString = function toString() {
    var str = util.format('[object BorayClient<host=%s>]', this.hostLabel);
    return (str);
};

/*
 * Given a cueball "backend", return a Cueball-compatible Connection object.
 * This is implemented by the separate FastConnection class.
 */
BorayClient.prototype.createFastConnection =
    function createFastConnection(backend) {
    assert.string(backend.key, 'backend.key');
    assert.string(backend.name, 'backend.name');
    assert.string(backend.address, 'backend.address');
    assert.number(backend.port, 'backend.port');

    return (new FastConnection({
        'address': backend.address,
        'collector': this.collector,
        'port': backend.port,
        'nRecentRequests': fastNRecentRequests,
        'tcpKeepAliveInitialDelay': dflClientTcpKeepAliveIdle,
        'log': this.log.child({
            'component': 'FastClient',
            'backendName': backend.name
        }),
        'crc_mode': this.crc_mode
    }));
};

/*
 * Internal functions for RPC contexts and context management
 *
 * Each RPC function receives as its first argument a BorayRpcContext, which is
 * a per-request handle for accessing configuration (like "unwrapErrors") and
 * the underlying Fast client.  When the RPC completes, the implementing
 * function must release the BorayRpcContext.  This mechanism enables us to
 * ensure that connections are never released twice from the same RPC, and it
 * also affords some debuggability if connections become leaked.  Additionally,
 * if future RPC function implementors need additional information from the
 * Boray client (e.g., a way to tell whether the caller has tried to cancel the
 * request), we can add additional functions to the BorayRpcContext.
 *
 * RPC functions use one of two patterns for obtaining and releasing RPC
 * contexts, depending on whether they're callback-based or event-emitter-based.
 * It's always possible for an RPC to fail because no RPC connections are
 * available, and these two mechanisms differ in how they handle that:
 *
 *    (1) Callback-based RPCs (e.g., getBucket) use this pattern:
 *
 *          rpcctx = this.ctxCreateForCallback(usercallback);
 *          if (rpcctx !== null) {
 *              callback = this.makeReleaseCb(rpcctx, usercallback);
 *              // Make the RPC call and invoke callback() upon completion.
 *          }
 *
 *        If a backend connection is available, a BorayRpcContext will be
 *        returned from ctxCreateForCallback().  These functions typically use
 *        makeReleaseCb() to wrap the user callback they were given with one
 *        that releases the RPC context before invoking the user callback.
 *
 *        If no backend connection is available, then callback() will be invoked
 *        asynchronously with an appropriate error, and the caller should not do
 *        anything else.
 *
 *    (2) Event-emitter-based RPCs (e.g., listBuckets) use this pattern:
 *
 *          rpcctx = this.ctxCreateForEmitter();
 *          if (rpcctx !== null) {
 *              ee = new EventEmitter();
 *              this.releaseWhenDone(rpcctx, ee);
 *              // Make the RPC call and emit 'end' or 'error' upon completion.
 *          } else {
 *              ee = emitUnavailable();
 *          }
 *
 *          return (ee);
 *
 *        If a backend connection is available, a BorayRpcContext will be
 *        returned from ctxCreateForEmitter().  These functions typically use
 *        releaseWhenDone() to release the RPC context when the event emitter
 *        emits '_boray_internal_rpc_done'.
 *
 *        If no backend connection is available, then the caller is responsible
 *        for allocating and returning a new EventEmitter that will emit the
 *        appropriate Error.
 */

/*
 * Internal function that returns a context used for RPC operations for a
 * callback-based RPC call.  If no backend connection is available, this
 * function returns null and schedules an asynchronous invocation of the given
 * callback with a suitable error.
 *
 * See "Internal functions for RPC contexts and context management" above.
 */
BorayClient.prototype.ctxCreateForCallback =
    function ctxCreateForCallback(callback) {
    var conn;

    assert.func(callback, 'callback');
    if (this.closeState !== BORAY_CS_OPEN) {
        setImmediate(callback, new Error('boray client has been closed'));
        return (null);
    }

    conn = this.pool.connAlloc();
    if (conn instanceof Error) {
        setImmediate(callback, conn);
        return (null);
    }

    return (this.ctxCreateCommon(conn));
};

/*
 * Internal function that returns a context used for RPC operations for an
 * event-emitter-based RPC call.  If no backend connection is available, this
 * function returns null and the caller is responsible for propagating the error
 * to its caller.
 *
 * See "Internal functions for RPC contexts and context management" above.
 */
BorayClient.prototype.ctxCreateForEmitter = function ctxCreateForEmitter() {
    var conn;

    if (this.closeState !== BORAY_CS_OPEN) {
        return (null);
    }

    conn = this.pool.connAlloc();
    if (conn instanceof Error) {
        /* The caller knows that this means there are no connections. */
        return (null);
    }

    return (this.ctxCreateCommon(conn));
};

/*
 * Internal function that creates an RPC context wrapping the given connection.
 * We keep track of outstanding RPC contexts to provide a clean close()
 * implementation and to aid debuggability in the event of leaks.
 */
BorayClient.prototype.ctxCreateCommon = function (conn) {
    var rpcctx;

    assert.object(conn);
    assert.ok(!(conn instanceof Error));
    assert.equal(this.closeState, BORAY_CS_OPEN);

    this.nactive++;

    rpcctx = new BorayRpcContext({
        'id': this.ncontexts++,
        'borayClient': this,
        'connection': conn
    });

    assert.ok(!this.activeContexts.hasOwnProperty(rpcctx.mc_id));
    this.activeContexts[rpcctx.mc_id] = rpcctx;
    return (rpcctx);
};

/*
 * Internal function for releasing an RPC context (that is, releasing the
 * underlying connection).
 */
BorayClient.prototype.ctxRelease = function ctxRelease(rpcctx) {
    assert.ok(this.nactive > 0);
    this.nactive--;

    assert.equal(this.activeContexts[rpcctx.mc_id], rpcctx);
    delete (this.activeContexts[rpcctx.mc_id]);
    this.pool.connRelease(rpcctx.mc_conn);

    if (this.nactive === 0 && this.closeState === BORAY_CS_CLOSING) {
        this.closeFini();
    }
};

/*
 * Given an RPC context and a user callback, return a callback that will
 * release the underlying RPC context and then invoke the user callback with
 * the same arguments.
 *
 * See "Internal functions for RPC contexts and context management" above.
 */
BorayClient.prototype.makeReleaseCb = function makeReleaseCb(rpcctx, cb) {
    var self = this;
    return (function onCallbackRpcComplete() {
        self.ctxRelease(rpcctx);
        cb.apply(null, arguments);
    });
};

/*
 * Given an RPC context and an event emitter, return a callback that will
 * release the underlying RPC context when the event emitter emits "end" or
 * "error".  This is the EventEmitter analog of makeReleaseCb.
 *
 * See "Internal functions for RPC contexts and context management" above.
 */
BorayClient.prototype.releaseWhenDone = function releaseOnEnd(rpcctx, emitter) {
    var self = this;
    var done = false;

    assert.object(rpcctx);
    assert.object(emitter);
    assert.ok(emitter instanceof EventEmitter);

    emitter.on('_boray_internal_rpc_done', function onEmitterRpcComplete() {
        assert.ok(!done);
        done = true;
        self.ctxRelease(rpcctx);
    });
};

/*
 * RPC implementation functions
 *
 * These are the primary public methods on the Boray client.  Typically, these
 * functions normalize and validate their arguments and then delegate to an
 * implementation in one of the nearby files.  They use one of the patterns
 * described above under "Internal functions for RPC contexts and context
 * management" to manage the RPC context.
 */

/**
 * Creates a bucket
 *
 * This function should be used when the virtual node for the bucket has already
 * been determined.
 *
 * @param {String} owner  - Account owner
 * @param {String} bucket - Bucket name
 * @param {Number} vnode  - Virtual node identifier
 * @param {Function} cb   - callback
 */
BorayClient.prototype.createBucket =
    function createBucket(owner, bucket, vnode, cb) {

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        buckets.createBucket(rpcctx, owner, bucket, vnode,
            this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Creates a bucket
 *
 * This function should be used when the virtual node for the bucket has not yet
 * been determined.
 *
 * @param {String} owner  - Account owner
 * @param {String} bucket - Bucket name
 * @param {Function} cb   - callback
 */
BorayClient.prototype.createBucketNoVnode =
    function createBucketNoVnode(owner, bucket, cb) {

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        buckets.createBucketNoVnode(rpcctx, owner, bucket,
            this.makeReleaseCb(rpcctx, cb));
    }
};


/**
 * Fetches a bucket
 *
 * This function should be used when the virtual node for the bucket has already
 * been determined.
 *
 * @param {String} owner  - Account owner
 * @param {String} bucket - Bucket name
 * @param {Number} vnode  - Virtual node identifier
 */
BorayClient.prototype.getBucket = function getBucket(owner, bucket, vnode, cb) {
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        buckets.getBucket(rpcctx, owner, bucket, vnode,
            this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Fetches a bucket
 *
 * This function should be used when the virtual node for the bucket has not yet
 * been determined.
 *
 * @param {String} owner  - Account owner
 * @param {String} bucket - Bucket name
 * @param {Function} cb - callback
 */
BorayClient.prototype.getBucketNoVnode =
    function getBucketNoVnode(owner, bucket, cb) {
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        buckets.getBucketNoVnode(rpcctx, owner, bucket,
            this.makeReleaseCb(rpcctx, cb));
};

/**
 * Lists buckets for an account at a specific virtual node
 *
 * @param {String} owner     - Account owner
 * @param {String} order_by  - An ordering clause for the resulting buckets
 * @param {String} prefix    - A prefix to use to group buckets
 * @param {Number} limit     - The maximum number of buckets to return
 * @param {Number} offset    - An starting offset into the buckets set
 * @param {Number} vnode     - Virtual node identifier
 */
BorayClient.prototype.listBuckets =
    function listBuckets(owner, order_by, prefix, limit, offset, vnode) {

    var rpcctx = this.ctxCreateForEmitter();
    var rv;

    if (!rpcctx) {
        return (emitUnavailable());
    }

    rv = buckets.listBuckets(rpcctx, owner, order_by, prefix, limit, offset,
        vnode);
    this.releaseWhenDone(rpcctx, rv);

    return (rv);
};

/**
 * Lists buckets for an account
 *
 * @param {String} owner     - Account owner
 * @param {Boolean} sorted   - Indicates if the results should be sorted
 * @param {String} order_by  - An ordering clause for the resulting buckets
 * @param {String} prefix    - A prefix to use to group buckets
 * @param {Number} limit     - The maximum number of buckets to return
 */
BorayClient.prototype.listBucketsNoVnode =
    function listBucketsNoVnode(owner, sorted, order_by, prefix, limit) {
    var rpcctx = this.ctxCreateForEmitter();
    var rv;

    if (!rpcctx) {
        return (emitUnavailable());
    }

    rv = buckets.listBucketsNoVnode(rpcctx, owner, sorted, order_by, prefix,
        limit);
    this.releaseWhenDone(rpcctx, rv);

    return (rv);
};


/**
 * Deletes a bucket
 *
 * This function should be used when the virtual node for the bucket has already
 * been determined.
 *
 * @param {String} owner  - Account owner
 * @param {String} bucket - Bucket name
 * @param {Number} vnode  - Virtual node identifier
 * @param {Function} cb   - callback
 */
BorayClient.prototype.deleteBucket = function deleteBucket(owner, bucket, vnode,
    cb) {
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        buckets.deleteBucket(rpcctx, owner, bucket, vnode,
            this.makeReleaseCb(rpcctx, cb));
    }
};


/**
 * Deletes a bucket
 *
 * This function should be used when the virtual node for the bucket has not yet
 * been determined.
 *
 * @param {String} owner  - Account owner
 * @param {String} bucket - Bucket name
 * @param {Function} cb   - callback
 */
BorayClient.prototype.deleteBucketNoVnode =
    function deleteBucketNoVnode(owner, bucket, cb) {

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        buckets.deleteBucketNoVnode(rpcctx, owner, bucket,
            this.makeReleaseCb(rpcctx, cb));
    }
};


/**
 * Creates an object. If an object already exists at the key it is overwritten.
 *
 * This function is used when the destination virtual node for the object has
 * already been determined.
 *
 * @param {String} owner           - Account owner
 * @param {String} bucket_id       - Bucket id
 * @param {String} name            - Object key name
 * @param {String} object_id       - Object id
 * @param {Number} content_length  - The size of the object
 * @param {String} content_md5     - MD5 digest of the object
 * @param {String} content_type    - Content-Type of the object
 * @param {Object} headers         - An object representing the HTTP headers for
 *                                   the object.
 * @param {Object} sharks          - An array of text strings that indicate the
 *                                   data centers and storage nodes where the
 *                                   data for the object resides.
 * @param {Object} props           - An object used to store unstructured data
 *                                   that may be shown to be important, but for
 *                                   which we are not immediately able to
 *                                   migrate the database to accommodate.
 * @param {Number} vnode           - Virtual node identifier
 * @param {Function} cb            - callback
 */
BorayClient.prototype.createObject = function createObject(owner, bucket_id,
    name, object_id, content_length, content_md5, content_type, headers,
    sharks, props, vnode, cb) {
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(content_length, 'content_length');
    assert.string(content_md5, 'content_md5');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.object(sharks, 'sharks');
    assert.number(vnode, 'vnode');
    assert.func(cb, 'callback');
    assert.optionalObject(props, 'props');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.createObject(rpcctx, owner, bucket_id, name, object_id,
            content_length, content_md5, content_type, headers, sharks, props,
            vnode, this.makeReleaseCb(rpcctx, cb));
    }
};


/**
 * Creates an object. If an object already exists at the key it is overwritten.
 *
 * This function is intended to be used when the destination virtual node for an
 * object has not been determined.
 *
 * @param {String} owner           - Account owner
 * @param {String} bucket_id       - Bucket id
 * @param {String} name            - Object key name
 * @param {String} object_id       - Object id
 * @param {Number} content_length  - The size of the object
 * @param {String} content_md5     - MD5 digest of the object
 * @param {String} content_type    - Content-Type of the object
 * @param {Object} headers         - An object representing the HTTP headers for
 *                                   the object.
 * @param {Object} sharks          - An array of text strings that indicate the
 *                                   data centers and storage nodes where the
 *                                   data for the object resides.
 * @param {Object} props           - An object used to store unstructured data
 *                                   that may be shown to be important, but for
 *                                   which we are not immediately able to
 *                                   migrate the database to accommodate.
 * @param {Function} cb            - callback
 */
BorayClient.prototype.createObjectNoVnode = function createObjectNoVnode(owner,
    bucket_id, name, object_id, content_length, content_md5, content_type,
    headers, sharks, props, cb) {

    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.string(object_id, 'object_id');
    assert.number(content_length, 'content_length');
    assert.string(content_md5, 'content_md5');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.object(sharks, 'sharks');
    assert.func(cb, 'callback');
    assert.optionalObject(props, 'props');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.createObjectNoVnode(rpcctx, owner, bucket_id, name, object_id,
            content_length, content_md5, content_type, headers, sharks, props,
            this.makeReleaseCb(rpcctx, cb));
    }
};


/**
 * Fetches an object.
 *
 * This function is intended to be used when the virtual node where the object
 * resides has already been determined.
 *
 * @param {String} owner           - Account owner
 * @param {String} bucket_id       - Bucket id
 * @param {String} name            - Object key name
 * @param {Number} vnode           - Virtual node identifier
 * @param {Function} cb            - callback
 */
BorayClient.prototype.getObject =
    function getObject(owner, bucket_id, name, vnode, cb) {

    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.getObject(rpcctx, owner, bucket_id, name, vnode,
            this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Fetches an object.
 *
 * This function is intended to be used when the virtual node where the object
 * resides has not been determined.
 *
 * @param {String} owner           - Account owner
 * @param {String} bucket_id       - Bucket id
 * @param {String} name            - Object key name
 * @param {Function} cb            - callback
 */
BorayClient.prototype.getObjectNoVnode =
    function getObjectNoVnode(owner, bucket_id, name, cb) {

    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.getObjectNoVnode(rpcctx, owner, bucket_id, name,
            this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Lists objects for the bucket belonging to the owner account at a particular
 * virtual node.
 *
 * @param {String} owner           - Account owner
 * @param {String} bucket_id       - Bucket id
 * @param {Number} vnode           - Virtual node identifier
 */
BorayClient.prototype.listObjects =
    function listObjects(owner, bucket_id, order_by, prefix, limit, offset,
        vnode) {

    var rpcctx = this.ctxCreateForEmitter();
    var rv;

    if (!rpcctx) {
        return (emitUnavailable());
    }

    rv = objects.listObjects(rpcctx, owner, bucket_id, order_by, prefix, limit,
        offset, vnode);
    this.releaseWhenDone(rpcctx, rv);

    return (rv);
};

/**
 * Lists objects for the bucket belonging to the owner account.
 *
 * @param {String} owner           - Account owner
 * @param {String} bucket_id       - Bucket id
 */
BorayClient.prototype.listObjectsNoVnode =
    function listObjectsNoVnode(owner, bucket_id, sorted, order_by, prefix,
        limit) {

    var rpcctx = this.ctxCreateForEmitter();
    var rv;

    if (!rpcctx) {
        return (emitUnavailable());
    }

    rv = objects.listObjectsNoVnode(rpcctx, owner, bucket_id, sorted, order_by,
        prefix, limit);
    this.releaseWhenDone(rpcctx, rv);

    return (rv);
};

/**
 * Deletes an object.
 *
 * This function is intended to be used when the virtual node where the object
 * resides has already been determined.
 *
 * @param {String} owner           - Account owner
 * @param {String} bucket_id       - Bucket id
 * @param {String} name            - Object key name
 * @param {Number} vnode           - Virtual node identifier
 * @param {Function} cb            - callback
 */
BorayClient.prototype.deleteObject =
    function deleteObject(owner, bucket_id, name, vnode, cb) {

    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.deleteObject(rpcctx, owner, bucket_id, name, vnode,
            this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Deletes an object.
 *
 * This function is intended to be used when the virtual node where the object
 * resides has not been determined.
 *
 * @param {String} owner           - Account owner
 * @param {String} bucket_id       - Bucket id
 * @param {String} name            - Object key name
 * @param {Function} cb            - callback
 */
BorayClient.prototype.deleteObjectNoVnode =
    function deleteObjectNoVnode(owner, bucket_id, name, cb) {

    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.deleteObjectNoVnode(rpcctx, owner, bucket_id, name,
            this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Performs a ping check against the server.
 *
 * Note that because the BorayClient is pooled and connects to all IPs in
 * a RR-DNS set, this actually just tells you that _one_ of the servers is
 * responding, not that all are.
 *
 * In most cases, you probably want to send in '{deep: true}' as options
 * so a DB-level check is performed on the server.
 *
 * @param {Object} opts   - request parameters
 * @return {EventEmitter} - listen for 'record', 'end' and 'error'
 */
BorayClient.prototype.ping = function _ping(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        meta.ping(rpcctx, opts, this.makeReleaseCb(rpcctx, cb));
};

/**
 * Query the API version of the server.
 *
 * Do not use this function except for reporting version numbers to humans.  See
 * the comment in meta.versionInternal().
 *
 * @param {Object} opts   - request parameters
 * @param {Function} cb   - callback
 */
BorayClient.prototype.versionInternal = function _version(opts, cb) {
    if (typeof (opts) === 'function') {
        cb = opts;
        opts = {};
    }
    assert.object(opts, 'options');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx)
        meta.versionInternal(rpcctx, opts, this.makeReleaseCb(rpcctx, cb));
};


/*
 * A BorayRpcContext is a per-request handle that refers back to the Boray
 * client and the underlying connection.  This object is provided to RPC
 * implementors, and allows them to access the underlying Fast client (in order
 * to make RPC requests), configuration (like "unwrapErrors"), and to release
 * the RPC context when the RPC completes.
 *
 * This class should be thought of as part of the implementation of the Boray
 * client itself, having internal implementation knowledge of the client.
 */
function BorayRpcContext(args) {
    assert.object(args, 'args');
    assert.number(args.id, 'args.id');
    assert.object(args.connection, 'args.connection');
    assert.object(args.borayClient, 'args.borayClient');

    /*
     * There's no mechanism in place to stop us from reaching this limit, but
     * even at one million requests per second, we won't hit it until the client
     * has been running for over 142 years.
     */
    assert.ok(args.id >= 0 && args.id < Math.pow(2, 53));

    this.mc_id = args.id;
    this.mc_conn = args.connection;
    this.mc_boray = args.borayClient;
}

BorayRpcContext.prototype.fastClient = function fastClient() {
    return (this.mc_conn.connection().fastClient());
};

BorayRpcContext.prototype.socketAddrs = function socketAddrs() {
    return (this.mc_conn.connection().socketAddrs());
};

BorayRpcContext.prototype.unwrapErrors = function unwrapErrors() {
    assert.bool(this.mc_boray.unwrapErrors);
    return (this.mc_boray.unwrapErrors);
};

BorayRpcContext.prototype.createLog = function createLog(options) {
    assert.optionalObject(options, 'options');
    options = jsprim.deepCopy(options || {});

    if (!options.req_id) {
        options.req_id = libuuid.create();
    }

    return (this.mc_boray.log.child(options, true));
};


///--- Exports

/*
 * Expose privateParseBorayParameters privately for testing, not for the outside
 * world.
 */
BorayClient.privateParseBorayParameters = parseBorayParameters;

module.exports = {
    Client: BorayClient
};
