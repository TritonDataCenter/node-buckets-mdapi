/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2019, Joyent, Inc.
 */

/*
 * lib/cmd.js: common functions used by command-line utilities
 *
 * Many of these functions follow a similar pattern: when there's a fatal error
 * (like failure to parse a numeric option), they emit a message to stderr and
 * then return false.  Callers use that to eventually print a usage message and
 * exit.
 */

var artedi = require('artedi');
var assert = require('assert-plus');
var bunyan = require('bunyan');
var getopt = require('posix-getopt');
var net = require('net');
var path = require('path');
var url = require('url');
var VError = require('verror');
var fprintf = require('extsprintf').fprintf;

/* Option string for options common to all commands */
var commonOptStr = 'Ab:(bootstrap-domain)h:(host)p:(port)S:(service)v';

/*
 * Currently, we only export parseCliOptions() and cliFinish().  If we find we
 * need more (or less) fine-grained control over option parsing or general CLI
 * execution, we can provide higher or lower-level interfaces here.
 */
exports.cliFinish = cliFinish;
exports.parseCliOptions = parseCliOptions;
exports.commonUsage = '[-A] [-b domain] [-h host] [-p port] [-S service] [-v]';

/*
 * Output artedi metrics if the -A flag was passed.  This function is run after
 * handling requests for each of the cmdline tools.  It will be passed the boray
 * client object, and if that client has a collector attached (which would be
 * added when -A was passed on the cmdline) it will write the artedi metrics to
 * stderr.
 *
 * It does not provide a return value.
 *
 * Named arguments:
 *
 *  cmdutil         A node-cmdutil object with a .fail() method.
 *                  cmdutil.fail(err) will be called if there are errors
 *                  collecting metrics.
 *
 *  client          Boray client object.
 *
 */
function cliFinish(cmdutil, client) {
    assert.object(client, 'client');
    assert.optionalObject(client.collector, 'client.collector');

    if (client.collector) {
        client.collector.collect(artedi.FMT_PROM,
            function _outputMetrics(err, metrics) {
                if (err) {
                    cmdutil.fail(err);
                    return;
                }
                console.error('[artedi metrics]');
                console.error(metrics);
            });
    }
}

/*
 * Parse command-line options and common environment variables.  This function
 * instantiates a node-getopt option parser that will handle the requested
 * command-line options plus the common options implemented in this file.
 * Common options are processed to fill in the "clientOptions" object described
 * below.  Each command-specific option is passed to the "onOption" callback
 * function described below.
 *
 * The returned value is the node-getopt parser itself, which allows callers to
 * access the index of the last option argument that was parsed.
 *
 * Named arguments:
 *
 *  argv            command-line arguments, including Node's leading two
 *                  arguments.  This would usually be `process.argv`.
 *
 *  env             process environment.  This would usually be `process.env`.
 *
 *  errstream       stream for error messages.  This would usually be
 *                  `process.stderr`.
 *
 *  extraOptStr     getopt-style option string for this command's custom
 *                  options.  This will be combined with the common option
 *                  string above and used to parse options with node-getopt.
 *
 *  clientOptions   an object that will be populated with arguments used to
 *                  create a Boray client.  That will include some combination
 *                  of "host", "port", and "srvDomain" as well as an appropriate
 *                  bunyan logger, configured based on the environment and
 *                  presence of "-v" options.  This may also contain
 *                  cueballOptions.
 *
 *  onUsage         function to be invoked when there's a usage error
 *
 *  onOption        function to be invoked for each getopt option parsed.  The
 *                  only argument is the option returned by node-getopt itself.
 *                  This field should be specified if and only if extraOptStr is
 *                  a non-empty string.  The function should handle unrecognized
 *                  options (usually by invoking the caller's usage function).
 *
 * This function uses the BORAY_SERVICE and BORAY_URL environment variables from
 * `env` to fill in common options where possible.  `LOG_LEVEL` is used to
 * configure the level of the bunyan logger.  Log verbosity is increased with
 * each instance of the "-v" option.
 */
function parseCliOptions(args) {
    var cmdName = path.basename(args.argv[1]);
    var parser, option, commonOpts, shortOpts;

    assert.object(args, 'args');
    assert.object(args.argv, 'args.argv');
    assert.object(args.env, 'args.env');
    assert.object(args.errstream, 'args.errstream');
    assert.string(args.extraOptStr, 'args.extraOptStr');
    assert.object(args.clientOptions, 'args.clientOptions');
    assert.func(args.onUsage, 'args.onUsage');
    assert.optionalFunc(args.onOption, 'args.onOption');

    if (args.extraOptStr === '') {
        assert.ok(typeof (args.onOption) != 'function');
    } else {
        assert.func(args.onUsage, 'args.onOption');
    }

    if (!args.clientOptions.log) {
        args.clientOptions.log = bunyan.createLogger({
            'name': cmdName,
            'level': (args.env.LOG_LEVEL || 'fatal'),
            'stream': process.stderr,
            'serializers': bunyan.stdSerializers
        });
    }

    /*
     * Validate that the extra option string doesn't try to override any of our
     * common options.  It's easiest to strip out long option names and then
     * check for the presence of any of our options.
     */
    shortOpts = args.extraOptStr;
    shortOpts = shortOpts.replace(/\([^)]*\)/g, '');
    commonOpts = commonOptStr;
    commonOpts = commonOpts.replace(/\([^)]*\)/g, '');
    commonOpts = commonOpts.replace(/:/g, '');
    commonOpts.split('').forEach(function (c) {
        if (shortOpts.indexOf(c) != -1) {
            throw (new VError('internal error: ' +
                'command cannot replace option: -%s', c));
        }
    });

    /*
     * Parse the combination option string.
     */
    parser = new getopt.BasicParser(commonOptStr + args.extraOptStr, args.argv);
    while ((option = parser.getopt()) !== undefined) {
        switch (option.option) {
        case 'A':
        case 'b':
        case 'h':
        case 'p':
        case 'S':
        case 'v':
            if (!parseCommonCliOption(args.errstream,
                args.clientOptions, { cmdName: cmdName }, option)) {
                args.onUsage();
            }
            break;

        default:
            if (args.extraOptStr === '') {
                args.onUsage();
            } else {
                args.onOption(option);
            }
            break;
        }
    }

    /*
     * For all of our commands, we use failFast by default so that the command
     * doesn't block indefinitely if Boray is down, and we use
     * mustCloseBeforeNormalProcessExit to make sure the commands clean up after
     * themselves.
     */
    args.clientOptions.failFast = true;
    args.clientOptions.mustCloseBeforeNormalProcessExit = true;

    /*
     * Perform final validation of the common options.
     */
    if (!finalizeCliOptions(args.errstream, args.clientOptions, args.env)) {
        args.onUsage();
    }

    return (parser);
}

/*
 * Parses one of the command-line options that's common to several commands.
 * See the option string at the top of this file.
 *
 * "clientOpts" is an object in which we're building the Boray client
 * configuration.
 *
 * "options" is an object with options for this function.  Currently only
 * supports the string options.cmdName, which represents the name of the tool
 * that is currently running for the cmdName label on artedi metrics.
 *
 * "option" is a node-getopt option object.
 *
 * If there is an error, prints an error message and returns false.
 */
function parseCommonCliOption(errstream, clientOpts, options, option) {
    var p, log;

    assert.object(errstream, 'errstream');
    assert.object(clientOpts, 'clientOpts');
    assert.object(options, 'options');
    assert.string(options.cmdName, 'options.cmdName');
    assert.object(option, 'option');

    switch (option.option) {
    case 'A':
        clientOpts.collector = artedi.createCollector({labels: {
            cmdName: options.cmdName
        }});
        break;

    case 'b':
        if (!clientOpts.hasOwnProperty('cueballOptions')) {
            clientOpts.cueballOptions = {};
        }

        clientOpts.cueballOptions.resolvers = [ option.optarg ];
        break;

    case 'h':
        clientOpts.host = option.optarg;
        break;

    case 'p':
        p = parseTcpPort(option.optarg);
        if (p === null) {
            fprintf(errstream, '-p/--port: expected valid TCP port\n');
            return (false);
        }
        clientOpts.port = p;
        break;

    case 'S':
        if (!validateSrvDomain(errstream, option.optarg)) {
            return (false);
        }

        clientOpts.srvDomain = option.optarg;
        break;

    case 'v':
        /*
         * This allows "-v" to be used multiple times and ensures that we
         * never wind up at a level less than TRACE.
         */
        log = clientOpts.log;
        log.level(Math.max(bunyan.TRACE, (log.level() - 10)));
        if (log.level() <= bunyan.DEBUG)
            log = log.child({src: true});
        break;

    default:
        throw (new Error('tried to parse non-common option'));
    }

    return (true);
}

/*
 * Performs final validation on CLI options and populates required arguments
 * with default values.  Like parseCommonCliOption(), on error this prints an
 * error message to stderr and returns false.
 */
function finalizeCliOptions(errstream, options, env) {
    /*
     * Independent of anything else, if the user specified a bootstrap domain in
     * the environment and didn't specify it on the CLI, incorporate that here.
     */
    if ((!options.hasOwnProperty('cueballOptions') ||
        !options.cueballOptions.hasOwnProperty('resolvers')) &&
        env['BORAY_BOOTSTRAP_DOMAIN']) {
        if (!options.hasOwnProperty('cueballOptions')) {
            options.cueballOptions = {};
        }

        options.cueballOptions.resolvers = [ env['BORAY_BOOTSTRAP_DOMAIN'] ];
    }

    if (options.srvDomain !== undefined) {
        /* The user specified -s/--service. */
        if (options.port !== undefined || options.host !== undefined) {
            fprintf(errstream, '-S/--service cannot be combined with ' +
                '-h/--host or -p/--port\n');
            return (false);
        }

        return (true);
    }

    if (options.host !== undefined && options.port !== undefined) {
        /* The user specified both -h/--host and -p/--port. */
        return (true);
    }

    if (options.host !== undefined || options.port !== undefined) {
        /*
         * The user specified one -h/--host and -p/--port.  Assume they want the
         * direct mode and fill in the other option from BORAY_URL or our
         * built-in default values.
         */
        return (populateDirectArguments(errstream, options, env));
    }

    /*
     * The user specified nothing on the command line.  Check for BORAY_SERVICE.
     */
    if (env['BORAY_SERVICE']) {
        if (!validateSrvDomain(errstream, env['BORAY_SERVICE'])) {
            return (false);
        }

        options.srvDomain = env['BORAY_SERVICE'];
        return (true);
    }

    /*
     * If we get this far, all that's left to try is BORAY_URL, then fall back
     * to built-in defaults.
     */
    return (populateDirectArguments(errstream, options, env));
}

function validateSrvDomain(errstream, domain) {
    if (net.isIP(domain)) {
        fprintf(errstream,
            'cannot use an IP address with -S/--service/BORAY_SERVICE\n');
        return (false);
    }

    return (true);
}

/*
 * Given a set of Boray client arguments, ensure that "host" and "port" are
 * populated based on BORAY_URL or our default values.  Like the other functions
 * in this file, on error, prints an error message and then returns "false" on
 * failure.
 *
 * Importantly, don't parse BORAY_URL if we're not going to use it.
 */
function populateDirectArguments(errstream, options, env) {
    var u, p;

    if (options.host === undefined || options.port === undefined) {
        /*
         * The user specified one of -h/--host and -p/--port, but not the other.
         */
        if (env['BORAY_URL']) {
            u = url.parse(env['BORAY_URL']);
            if (options.host === undefined) {
                options.host = u['hostname'];
            }

            if (options.port === undefined && u['port'] !== null) {
                p = parseTcpPort(u['port']);
                if (p === null) {
                    fprintf(errstream,
                        'port in BORAY_URL is not a valid TCP port\n');
                    return (false);
                }

                options.port = p;
            }
        }

        if (options.host === undefined) {
            options.host = '127.0.0.1';
        }

        if (options.port === undefined) {
            options.port = 2020;
        }
    }

    return (true);
}

function parseTcpPort(portstr) {
    var p;

    assert.string(portstr, 'portstr');
    p = parseInt(portstr, 10);
    if (isNaN(p) || p < 0 || p >= 65536) {
        return (null);
    }

    return (p);
}
