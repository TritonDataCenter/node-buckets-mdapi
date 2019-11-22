/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2020 Joyent, Inc.
 */

/*
 * lib/client.js: BucketsMdapi client implementation.  The BucketsMdapiClient
 * object is the handle through which consumers make RPC requests to a remote
 * BucketsMdapi server.
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

var BucketsMdapiConnectionPool = require('./pool');
var FastConnection = require('./fast_connection');
var buckets = require('./buckets');
var meta = require('./meta');
var objects = require('./objects');
var parseBucketsMdapiParameters =
    require('./client_params').parseBucketsMdapiParameters;
var rpc = require('./rpc');

///--- Default values for function arguments

var fastNRecentRequests         = 5;
var dflClientTcpKeepAliveIdle   = 10000;  /* milliseconds */

/*
 * See BucketsMdapiClient() constructor.
 */
var BUCKETS_MDAPI_CS_OPEN    = 'open';
var BUCKETS_MDAPI_CS_CLOSING = 'closing';


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
 * Constructor for the buckets-mdapi client.
 *
 * This client uses the cueball module to maintain a pool of TCP connections to
 * the IP addresses associated with a DNS name.  cueball is responsible for
 * DNS resolution (periodically, in the background) and maintaining the
 * appropriate TCP connections, while we maintain a small abstraction for
 * balancing requests across connections.
 *
 * The options accepted, the constraints on them, and several examples are
 * described in the buckets-mdapi(3) manual page inside this repository.
 * Callers can also specify any number of legacy options documented with
 * populateLegacyOptions().
 */
function BucketsMdapiClient(options) {
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

    coptions = parseBucketsMdapiParameters(options);
    cueballOptions = coptions.cueballOptions;

    /* Read-only metadata used for toString() and the like. */
    this.hostLabel = coptions.label;

    this.unwrapErrors = options.unwrapErrors ? true : false;
    this.failFast = options.failFast ? true : false;
    this.requireIndexes = options.requireIndexes ? true : false;
    this.requireOnlineReindexing =
        options.requireOnlineReindexing ? true : false;

    /* Helper objects. */
    this.log = options.log.child({
        component: 'BucketsMdapiClient',
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
        throw new VError(resolver,
            'invalid buckets-mdapi client configuration');
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
     *     BUCKETS_MDAPI_CS_OPEN        close() has never been invoked
     *
     *     BUCKETS_MDAPI_CS_CLOSING     close() has been invoked, but we have
     *                          not finished closing (presumably because
     *                          outstanding requests have not yet aborted)
     *
     *     BUCKETS_MDAPI_CS_CLOSED      close process has completed and there
     *                          are no connections in use any more.
     */
    this.closeState = BUCKETS_MDAPI_CS_OPEN;    /* see above */
    this.nactiveAtClose = null;         /* value of "nactive" at close() */

    /*
     * If requested, add a handler to ensure that the process does not exit
     * without this client being closed.
     */
    if (options.mustCloseBeforeNormalProcessExit) {
        this.onprocexit = function processExitCheck(code) {
            if (code === 0) {
                throw (new Error('process exiting before buckets-madpi client' +
                    ' closed'));
            }
        };
        process.on('exit', this.onprocexit);
    } else {
        this.onprocexit = null;
    }

    this.pool = new BucketsMdapiConnectionPool({
        'log': this.log,
        'cueballResolver': this.cueballResolver,
        'cueballSet': this.cueball
    });

    this.cueballOnStateChange = function (st) {
        self.onCueballStateChange(st);
    };

    this.cueball.on('stateChanged', this.cueballOnStateChange);
}

util.inherits(BucketsMdapiClient, EventEmitter);

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
Object.defineProperty(BucketsMdapiClient.prototype, 'connected', {
    'get': function () {
        return (this.timeConnected !== null &&
            this.closeState === BUCKETS_MDAPI_CS_OPEN);
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
BucketsMdapiClient.prototype.onCueballStateChange =
    function onCueballStateChange(st) {
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
        this.cueball.removeListener('stateChanged',
            this.cueballOnStateChange);
        err = new VError('buckets-mdapi client "%s": failed to establish ' +
            'connection', this.hostLabel);
        this.log.warn(err);
        this.emit('error', err);
        this.close();
    }
};

/**
 * Aborts outstanding requests, shuts down all connections, and closes this
 * client down.
 */
BucketsMdapiClient.prototype.close = function close() {
    var self = this;

    if (this.closeState !== BUCKETS_MDAPI_CS_OPEN) {
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

    this.closeState = BUCKETS_MDAPI_CS_CLOSING;
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

BucketsMdapiClient.prototype.closeFini = function closeFini() {
    assert.equal(this.closeState, BUCKETS_MDAPI_CS_CLOSING);
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


BucketsMdapiClient.prototype.toString = function toString() {
    var str = util.format('[object BucketsMdapiClient<host=%s>]',
        this.hostLabel);
    return (str);
};

/*
 * Given a cueball "backend", return a Cueball-compatible Connection object.
 * This is implemented by the separate FastConnection class.
 */
BucketsMdapiClient.prototype.createFastConnection =
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
        })
    }));
};

/*
 * Internal functions for RPC contexts and context management
 *
 * Each RPC function receives as its first argument a BucketsMdapiRpcContext,
 * which is a per-request handle for accessing configuration (like
 * "unwrapErrors") and the underlying Fast client.  When the RPC completes, the
 * implementing function must release the BucketsMdapiRpcContext.  This
 * mechanism enables us to ensure that connections are never released twice from
 * the same RPC, and it also affords some debuggability if connections become
 * leaked.  Additionally, if future RPC function implementors need additional
 * information from the BucketsMdapi client (e.g., a way to tell whether the
 * caller has tried to cancel the request), we can add additional functions to
 * the BucketsMdapiRpcContext.
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
 *        If a backend connection is available, a BucketsMdapiRpcContext will be
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
 *        If a backend connection is available, a BucketsMdapiRpcContext will be
 *        returned from ctxCreateForEmitter().  These functions typically use
 *        releaseWhenDone() to release the RPC context when the event emitter
 *        emits '_buckets_mdapi_internal_rpc_done'.
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
BucketsMdapiClient.prototype.ctxCreateForCallback =
    function ctxCreateForCallback(callback) {
    var conn;

    assert.func(callback, 'callback');
    if (this.closeState !== BUCKETS_MDAPI_CS_OPEN) {
        setImmediate(callback,
            new Error('buckets-mdapi client has been closed'));
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
BucketsMdapiClient.prototype.ctxCreateForEmitter =
    function ctxCreateForEmitter() {
    var conn;

    if (this.closeState !== BUCKETS_MDAPI_CS_OPEN) {
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
BucketsMdapiClient.prototype.ctxCreateCommon = function (conn) {
    var rpcctx;

    assert.object(conn);
    assert.ok(!(conn instanceof Error));
    assert.equal(this.closeState, BUCKETS_MDAPI_CS_OPEN);

    this.nactive++;

    rpcctx = new BucketsMdapiRpcContext({
        'id': this.ncontexts++,
        'bucketsMdapiClient': this,
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
BucketsMdapiClient.prototype.ctxRelease = function ctxRelease(rpcctx) {
    assert.ok(this.nactive > 0);
    this.nactive--;

    assert.equal(this.activeContexts[rpcctx.mc_id], rpcctx);
    delete (this.activeContexts[rpcctx.mc_id]);
    this.pool.connRelease(rpcctx.mc_conn);

    if (this.nactive === 0 && this.closeState === BUCKETS_MDAPI_CS_CLOSING) {
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
BucketsMdapiClient.prototype.makeReleaseCb =
    function makeReleaseCb(rpcctx, cb) {
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
BucketsMdapiClient.prototype.releaseWhenDone =
    function releaseOnEnd(rpcctx, emitter) {
    var self = this;
    var done = false;

    assert.object(rpcctx);
    assert.object(emitter);
    assert.ok(emitter instanceof EventEmitter);

    emitter.on('_buckets_mdapi_internal_rpc_done',
        function onEmitterRpcComplete() {
        assert.ok(!done);
        done = true;
        self.ctxRelease(rpcctx);
    });
};

/*
 * RPC implementation functions
 *
 * These are the primary public methods on the BucketsMdapi client.  Typically,
 * these functions normalize and validate their arguments and then delegate to
 * an implementation in one of the nearby files.  They use one of the patterns
 * described above under "Internal functions for RPC contexts and context
 * management" to manage the RPC context.
 */

/**
 * Fetches the metadata placement data managed by buckets-mdplacement service
 */
BucketsMdapiClient.prototype.getPlacementData = function getPlacementData(cb) {
    assert.func(cb, 'cb');

    var rpcctx = this.ctxCreateForCallback(cb);
    var callback = this.makeReleaseCb(rpcctx, cb);

    if (rpcctx) {
        var log = rpc.childLogger(rpcctx, {});

        rpc.rpcCommonBufferData({
            rpcctx: rpcctx,
            rpcmethod: 'getplacementdata',
            rpcargs: [],
            log: log
        }, function (err, placementData) {
            if (err) {
                callback(err);
                return;
            }

            if (placementData.length !== 1) {
                err = new VError(
                    'bad server response: expected 1 JSON object, found %d',
                    placementData.length);
                callback(err);
                return;
            }

            callback(null, placementData[0]);
        });
    }
};

/**
 * Creates a bucket
 *
 * This function should be used when the virtual node for the bucket has already
 * been determined.
 *
 * @param {String} owner  - Account owner
 * @param {String} bucket - Bucket name
 * @param {Number} vnode  - Virtual node identifier
 * @param {String} req_id - Request identifier
 * @param {Function} cb   - callback
 */
BucketsMdapiClient.prototype.createBucket =
    function createBucket(owner, bucket, vnode, req_id, cb) {

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        buckets.createBucket(rpcctx, owner, bucket, vnode, req_id,
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
 * @param {String} req_id - Request identifier
 */
BucketsMdapiClient.prototype.getBucket =
    function getBucket(owner, bucket, vnode,
    req_id, cb) {
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        buckets.getBucket(rpcctx, owner, bucket, vnode, req_id,
            this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Lists buckets for an account at a specific virtual node
 *
 * @param {String} owner     - Account owner
 * @param {String} prefix    - A prefix to use to group buckets
 * @param {Number} limit     - The maximum number of buckets to return
 * @param {Number} marker    - An optional string to start listing from
 * @param {Number} vnode     - Virtual node identifier
 * @param {String} req_id    - Request identifier
 */
BucketsMdapiClient.prototype.listBuckets =
    function listBuckets(owner, prefix, limit, marker, vnode, req_id) {

    var rpcctx = this.ctxCreateForEmitter();
    var rv;

    if (!rpcctx) {
        return (emitUnavailable());
    }

    rv = buckets.listBuckets(rpcctx, owner, prefix, limit, marker, vnode,
        req_id);
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
 * @param {String} req_id - Request identifier
 * @param {Function} cb   - callback
 */
BucketsMdapiClient.prototype.deleteBucket =
    function deleteBucket(owner, bucket, vnode,
    req_id, cb) {
    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        buckets.deleteBucket(rpcctx, owner, bucket, vnode, req_id,
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
 * @param {String} req_id          - Request identifier
 * @param {Function} cb            - callback
 */
BucketsMdapiClient.prototype.createObject =
    function createObject(owner, bucket_id,
    name, object_id, content_length, content_md5, content_type, headers,
    sharks, props, vnode, req_id, cb) {
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(content_length, 'content_length');
    assert.string(content_md5, 'content_md5');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.object(sharks, 'sharks');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');
    assert.optionalObject(props, 'props');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.createObject(rpcctx, owner, bucket_id, name, object_id,
            content_length, content_md5, content_type, headers, sharks, props,
            vnode, req_id, this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Updates an object's metadata. If no object with the given name exists for the
 * account then an error is returned indicating the object was not found.
 *
 * This function is used when the destination virtual node for the object has
 * already been determined.
 *
 * @param {String} owner           - Account owner
 * @param {String} bucket_id       - Bucket id
 * @param {String} name            - Object key name
 * @param {String} object_id       - Object id
 * @param {String} content_type    - Content-Type of the object
 * @param {Object} headers         - An object representing the HTTP headers for
 *                                   the object.
 * @param {Object} props           - An object used to store unstructured data
 *                                   that may be shown to be important, but for
 *                                   which we are not immediately able to
 *                                   migrate the database to accommodate.
 * @param {Number} vnode           - Virtual node identifier
 * @param {String} req_id          - Request identifier
 * @param {Function} cb            - callback
 */
BucketsMdapiClient.prototype.updateObject =
    function updateObject(owner, bucket_id,
    name, object_id, content_type, headers, props, vnode, req_id, cb) {
    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.string(content_type, 'content_type');
    assert.object(headers, 'headers');
    assert.number(vnode, 'vnode');
    assert.optionalObject(props, 'props');
    assert.string(req_id, 'req_id');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.updateObject(rpcctx, owner, bucket_id, name, object_id,
        content_type, headers, props, vnode, req_id,
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
 * @param {String} req_id          - Request identifier
 * @param {Function} cb            - callback
 */
BucketsMdapiClient.prototype.getObject =
    function getObject(owner, bucket_id, name, vnode, req_id, cb) {

    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.getObject(rpcctx, owner, bucket_id, name, vnode, req_id,
            this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Lists objects for the bucket belonging to the owner account at a particular
 * virtual node.
 *
 * @param {String} owner      - Account owner
 * @param {String} bucket_id  - Bucket id
 * @param {String} prefix     - A prefix to use to group buckets
 * @param {Number} limit      - The maximum number of buckets to return
 * @param {String} marker     - An optional string to start listing from
 * @param {Number} vnode      - Virtual node identifier
 * @param {String} req_id     - Request identifier
 */
BucketsMdapiClient.prototype.listObjects =
    function listObjects(owner, bucket_id, prefix, limit, marker, vnode,
        req_id) {

    var rpcctx = this.ctxCreateForEmitter();
    var rv;

    if (!rpcctx) {
        return (emitUnavailable());
    }

    rv = objects.listObjects(rpcctx, owner, bucket_id, prefix, limit,
        marker, vnode, req_id);
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
 * @param {String} req_id          - Request identifier
 * @param {Function} cb            - callback
 */
BucketsMdapiClient.prototype.deleteObject =
    function deleteObject(owner, bucket_id, name, vnode, req_id, cb) {

    assert.string(owner, 'owner');
    assert.string(bucket_id, 'bucket_id');
    assert.string(name, 'name');
    assert.number(vnode, 'vnode');
    assert.string(req_id, 'req_id');
    assert.func(cb, 'callback');

    var rpcctx = this.ctxCreateForCallback(cb);
    if (rpcctx) {
        objects.deleteObject(rpcctx, owner, bucket_id, name, vnode, req_id,
            this.makeReleaseCb(rpcctx, cb));
    }
};

/**
 * Performs a ping check against the server.
 *
 * Note that because the BucketsMdapiClient is pooled and connects to all IPs in
 * a RR-DNS set, this actually just tells you that _one_ of the servers is
 * responding, not that all are.
 *
 * In most cases, you probably want to send in '{deep: true}' as options
 * so a DB-level check is performed on the server.
 *
 * @param {Object} opts   - request parameters
 * @return {EventEmitter} - listen for 'record', 'end' and 'error'
 */
BucketsMdapiClient.prototype.ping = function _ping(opts, cb) {
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
BucketsMdapiClient.prototype.versionInternal = function _version(opts, cb) {
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


/**
 * Gets a batch of garbage.
 *
 * @param {Object} opts              - request parameters
 * @param {String} opts.request_id   - request uuid
 * @param {Function} cb              - callback
 */
BorayClient.prototype.getGarbageBatch = function getGarbageBatch(opts, cb) {
    assert.object(opts, 'opts');
    assert.uuid(opts.request_id, 'opts.request_id');
    assert.func(cb, 'cb');

    var rpcctx = this.ctxCreateForCallback(cb);
    var callback = this.makeReleaseCb(rpcctx, cb);

    if (rpcctx) {
        var log = rpc.childLogger(rpcctx, {});

        rpc.rpcCommonBufferData({
            rpcctx: rpcctx,
            rpcmethod: 'getgcbatch',
            rpcargs: [ {request_id: opts.request_id} ],
            log: log
        }, function (err, garbageBatch) {
            if (err) {
                callback(err);
                return;
            }

            if (garbageBatch.length !== 1) {
                err = new VError(
                    'bad server response: expected 1 JSON object, found %d',
                    garbageBatch.length);
                callback(err);
                return;
            }

            callback(null, garbageBatch[0]);
        });
    }
};


/* BEGIN JSSTYLED */
/**
 * Deletes a batch of garbage.
 *
 * @param {Object} opts              - request parameters
 * @param {String} opts.batch_id     - batch id previously returned by getGarbageBatch
 * @param {String} opts.request_id   - request uuid
 * @param {Function} cb              - callback
 */
/* END JSSTYLED */
BorayClient.prototype.deleteGarbageBatch =
function deleteGarbageBatch(opts, cb) {
    assert.object(opts, 'opts');
    assert.uuid(opts.batch_id, 'opts.batch_id');
    assert.uuid(opts.request_id, 'opts.request_id');
    assert.func(cb, 'cb');

    var rpcctx = this.ctxCreateForCallback(cb);
    var callback = this.makeReleaseCb(rpcctx, cb);

    if (rpcctx) {
        var log = rpc.childLogger(rpcctx, {});

        rpc.rpcCommonBufferData({
            rpcctx: rpcctx,
            rpcmethod: 'deletegcbatch',
            rpcargs: [ {request_id: opts.request_id, batch_id: opts.batch_id} ],
            log: log
        }, function (err, deleteResult) {
            if (err) {
                callback(err);
                return;
            }

            if (deleteResult.length !== 1) {
                err = new VError(
                    'bad server response: expected 1 JSON object, found %d',
                    deleteResult.length);
                callback(err);
                return;
            }

            callback(null, deleteResult[0]);
        });
    }
};


/*
 * A BucketsMdapiRpcContext is a per-request handle that refers back to the
 * BucketsMdapi client and the underlying connection.  This object is provided
 * to RPC implementors, and allows them to access the underlying Fast client (in
 * order to make RPC requests), configuration (like "unwrapErrors"), and to
 * release the RPC context when the RPC completes.
 *
 * This class should be thought of as part of the implementation of the
 * BucketsMdapi client itself, having internal implementation knowledge of the
 * client.
 */
function BucketsMdapiRpcContext(args) {
    assert.object(args, 'args');
    assert.number(args.id, 'args.id');
    assert.object(args.connection, 'args.connection');
    assert.object(args.bucketsMdapiClient, 'args.bucketsMdapiClient');

    /*
     * There's no mechanism in place to stop us from reaching this limit, but
     * even at one million requests per second, we won't hit it until the client
     * has been running for over 142 years.
     */
    assert.ok(args.id >= 0 && args.id < Math.pow(2, 53));

    this.mc_id = args.id;
    this.mc_conn = args.connection;
    this.mc_buckets_mdapi = args.bucketsMdapiClient;
}

BucketsMdapiRpcContext.prototype.fastClient = function fastClient() {
    return (this.mc_conn.connection().fastClient());
};

BucketsMdapiRpcContext.prototype.socketAddrs = function socketAddrs() {
    return (this.mc_conn.connection().socketAddrs());
};

BucketsMdapiRpcContext.prototype.unwrapErrors = function unwrapErrors() {
    assert.bool(this.mc_buckets_mdapi.unwrapErrors);
    return (this.mc_buckets_mdapi.unwrapErrors);
};

BucketsMdapiRpcContext.prototype.createLog = function createLog(options) {
    assert.optionalObject(options, 'options');
    options = jsprim.deepCopy(options || {});

    if (!options.req_id) {
        options.req_id = libuuid.create();
    }

    return (this.mc_buckets_mdapi.log.child(options, true));
};


///--- Exports

/*
 * Expose privateParseBucketsMdapiParameters privately for testing, not for the
 * outside world.
 */
BucketsMdapiClient.privateParseBucketsMdapiParameters =
    parseBucketsMdapiParameters;

module.exports = {
    Client: BucketsMdapiClient
};
