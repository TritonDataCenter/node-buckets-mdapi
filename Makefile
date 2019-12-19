#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright 2019 Joyent, Inc.
#

#
# Makefile: basic Makefile for template API service
#
# This Makefile is a template for new repos. It contains only repo-specific
# logic and uses included makefiles to supply common targets (javascriptlint,
# jsstyle, restdown, etc.), which are used by other repos as well. You may well
# need to rewrite most of this file, but you shouldn't need to touch the
# included makefiles.
#
# If you find yourself adding support for new targets that could be useful for
# other projects too, you should add these to the original versions of the
# included Makefiles (in eng.git) so that other teams can use them too.
#

#
# Tools
#
NODE			:= node
NPM			:= npm

#
# Files
#
JS_FILES	:= $(shell find lib -name '*.js')
JS_FILES	+= $(wildcard bin/*)
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE   = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS    = -f tools/jsstyle.conf

include ./tools/mk/Makefile.defs

#
# Repo-specific targets
#
.PHONY: all
all: $(REPO_DEPS)
	$(NPM) rebuild

# "Cutting a release" is just tagging the current commit with
# "v(package.json version)".
.PHONY: cutarelease
cutarelease:
	which json 2>/dev/null 1>/dev/null  # Ensure have the 'json' tool.
	ver=$(shell json -f package.json version) && \
	    git tag "v$$ver" && \
	    git push origin "v$$ver"

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.targ

