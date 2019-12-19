/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright 2019 Joyent, Inc.
 */

/*
 * lib/index.js: public exports from the node-buckets-mdapi module.
 */

var Client = require('./client').Client;


///-- API

module.exports = {
    Client: Client,
    createClient: function createClient(options) {
        return (new Client(options));
    }
};
