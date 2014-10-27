#!/usr/bin/env python
#    Copyright Reliance Jio Infocomm, Ltd.
#    Author: Soren Hansen <Soren.Hansen@ril.com>
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
import argparse
import errno
import sys
import socket
import time
import urllib3
import urlparse
import os
import netifaces
import re
import json
import yaml
import consulate
from urllib3.exceptions import HTTPError

class DeploymentOrchestrator(object):
    UPDATE_AVAILABLE = 0
    UP_TO_DATE = 1
    NO_CLUE = 2
    NO_CLUE_BUT_WERE_JUST_GETTING_STARTED = 3

    def __init__(self, host='127.0.0.1', port=9500):
        self.host = host
        self.port = port
        self._consul = None

    @property
    def consul(self):
        if not self._consul:
            self._consul = session = consulate.Consulate(self.host, self.port)
        return self._consul

    def trigger_update(self, new_version):
        self.consul.kv['/current_version'] = new_version

    def pending_update(self):
        local_version = self.local_version()
        try:
            if (self.current_version() == local_version):
                return self.UP_TO_DATE
            else:
                return self.UPDATE_AVAILABLE
        except:
            if local_version:
                return self.NO_CLUE
            else:
                return self.NO_CLUE_BUT_WERE_JUST_GETTING_STARTED

    def current_version(self):
        try:
            return str(self.consul.kv['/current_version']).strip()
        except KeyError:
            return "No version set"

    # TODO this does not work yet...
    def ping(self):
        try:
            return bool(self.consul.machines)
        except (consul.EtcdError, consul.EtcdException, HTTPError):
            return False

    def update_own_status(self, hostname, status_type, status_result):
        status_dir = '/status/%s' % status_type
        if status_type == 'puppet':
            if int(status_result) in (4, 6, 1):
                status_dir = '/status/puppet/failed'
                delete_dirs = ['/status/puppet/success', '/status/puppet/pending']
            elif int(status_result) == -1:
                status_dir = '/status/puppet/pending'
                delete_dirs = ['/status/puppet/success', '/status/puppet/failed']
            else:
                status_dir = '/status/puppet/success'
                delete_dirs = ['/status/puppet/failed', '/status/puppet/pending']
        elif status_type == 'validation':
            if int(status_result) == 0:
                status_dir = '/status/validation/success'
                delete_dirs = ['/status/validation/failed']
            else:
                status_dir = '/status/validation/failed'
                delete_dirs = ['/status/validation/success']
        else:
            raise Exception('Invalid status_type:%s' % status_type)

        self.consul.kv['%s/%s' % (status_dir, hostname)] = str(time.time())

        for delete_dir in delete_dirs:
            try:
                keyname = '%s/%s' % (delete_dir, hostname)
                print "Deleting %s" % keyname
                del(self.consul.kv[keyname])
            except KeyError:
                pass
        return True

    # this is not removing outdated versions?
    def update_own_info(self, hostname, version=None):
        version = version or self.local_version()
        if not version:
            return
        version_dir = '/running_version/%s' % version
        self.consul.kv['%s/%s' % (version_dir, hostname)] = str(time.time())

    # this call may not scale
    # if pulls down all host version records as
    # a single hash
    def running_versions(self):
        try:
            res = self.consul.kv.find('/running_version')
            return set([x.split('/')[-2] for x in res])
        except KeyError:
            return []

    # this call may not scale
    # if pulls down all host version records as
    # a single hash
    def hosts_at_version(self, version):
        version_dir = '/running_version/%s' % (version,)
        try:
            res = self.consul.kv.find(version_dir)
        except KeyError:
            return []
        result_set = set()
        for x in res:
            if x.split('/')[-2] == version:
                result_set.add(x.split('/')[-1])
        return result_set

    def get_failures(self, hosts):
        failures = {}
        for i in ['validation', 'puppet']:
            try:
                dir = '/status/%s/failed' % (i)
                failures[i] = self.consul.kv.find(dir).keys()
            except KeyError:
                failures[i] = []
            if hosts:
                for host in failures[i]:
                    print '%s failure:%s' % (i.capitalize(), os.path.basename(host))
            else:
                print "%s failures :%s" % (i.capitalize(),len(failures[i]))
        return len(failures['puppet']) == 0 and len(failures['validation']) == 0

    def verify_hosts(self, version, hosts):
        return set(hosts).issubset(self.hosts_at_version(version))

    def check_single_version(self, version, verbose=False):
        running_versions = self.running_versions()
        unwanted_versions = filter(lambda x: x != version,
                                   running_versions)
        wanted_version_found = version in running_versions
        if verbose:
            print 'Wanted version found:', wanted_version_found
            print 'Unwanted versions found:', ', '.join(unwanted_versions)
        return wanted_version_found and not unwanted_versions

    def local_version(self, new_value=None):
        mode = new_value is None and 'r' or 'w'

        try:
            with open('/etc/current_version', mode) as fp:
                if new_value is None:
                    return fp.read().strip()
                else:
                    fp.write(new_value)
                    return new_value
        except IOError, e:
            if e.errno == errno.ENOENT:
                return ''
            raise


def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='Utility for '
                                                 'orchestrating updates')
    parser.add_argument('--host', type=str,
                        default='127.0.0.1', help="local consul agent")
    parser.add_argument('--port', type=int, default=8500, help="consul port")
    subparsers = parser.add_subparsers(dest='subcmd')

    trigger_parser = subparsers.add_parser('trigger_update',
                                           help='Trigger an update')
    trigger_parser.add_argument('version', type=str, help='Version to deploy')

    current_version_parser = subparsers.add_parser('current_version',
                                                   help='Get available version')

    ping_parser = subparsers.add_parser('ping', help='Ping consul')

    pending_update = subparsers.add_parser('pending_update',
                                           help='Check for pending update')

    local_version_parser = subparsers.add_parser('local_version',
                                                 help='Get or set local version')
    local_version_parser.add_argument('version', nargs='?', help="If given, set this as the local version")
    update_own_status_parser = subparsers.add_parser('update_own_status', help="Update info related to the current status of a host")
    update_own_status_parser.add_argument('--hostname', type=str, default=socket.gethostname(),
                                          help="This system's hostname")
    update_own_status_parser.add_argument('status_type', type=str, help="Type of status to update")
    update_own_status_parser.add_argument('status_result', type=int, help="Command exit code used to derive status")
    list_failures_parser = subparsers.add_parser('get_failures', help="Return a list of every failed host. Returns the number of hosts in a failed state")
    list_failures_parser.add_argument('--hosts', action='store_true', help="list out all hosts in each state and not just the number in each state")
    update_own_info_parser = subparsers.add_parser('update_own_info', help="Update host's own info")
    update_own_info_parser.add_argument('--hostname', type=str, default=socket.gethostname(),
                                        help="This system's hostname")
    update_own_info_parser.add_argument('--version', type=str,
                                        help="Override version to report into consul")

    running_versions_parser = subparsers.add_parser('running_versions', help="List currently running versions")
    hosts_at_version_parser = subparsers.add_parser('hosts_at_version', help="List hosts at specified version")
    hosts_at_version_parser.add_argument('version', type=str, help="Version to retrieve list of hosts for")

    verify_hosts_parser = subparsers.add_parser('verify_hosts', help="Verify that list of hosts are all available")
    verify_hosts_parser.add_argument('version', help="Version to look for")

    check_single_version_parser = subparsers.add_parser('check_single_version', help="Check if the given version is the only one currently running")
    check_single_version_parser.add_argument('version', help='The version to check for')
    check_single_version_parser.add_argument('--verbose', '-v', action='store_true', help='Be verbose')
    args = parser.parse_args(argv)

    do = DeploymentOrchestrator(args.host, args.port)
    if args.subcmd == 'trigger_update':
        do.trigger_update(args.version)
    elif args.subcmd == 'current_version':
        print do.current_version()
    elif args.subcmd == 'check_single_version':
        sys.exit(not do.check_single_version(args.version, args.verbose))
    elif args.subcmd == 'update_own_status':
        do.update_own_status(args.hostname, args.status_type, args.status_result)
    elif args.subcmd == 'update_own_info':
        do.update_own_info(args.hostname, version=args.version)
    elif args.subcmd == 'ping':
        did_it_work = do.ping()
        if did_it_work:
            print 'Connection succesful'
            return 0
        else:
            print 'Connection failed'
            return 1
    elif args.subcmd == 'local_version':
        print do.local_version(args.version)
    elif args.subcmd == 'running_versions':
        print '\n'.join(do.running_versions())
    elif args.subcmd == 'hosts_at_version':
        print '\n'.join(do.hosts_at_version(args.version))
    elif args.subcmd == 'verify_hosts':
        buffer = sys.stdin.read().strip()
        hosts = buffer.split('\n')
        return not do.verify_hosts(args.version, hosts)
    elif args.subcmd == 'get_failures':
        return not do.get_failures(args.hosts)
    elif args.subcmd == 'pending_update':
        pending_update = do.pending_update()
        msg = {do.UPDATE_AVAILABLE: "Yes, there is an update pending",
               do.UP_TO_DATE: "No updates pending",
               do.NO_CLUE: "Could not get current_version",
               do.NO_CLUE_BUT_WERE_JUST_GETTING_STARTED: "Could not get current_version, but there's also no local version set"
               }[pending_update]
        print msg
        return pending_update

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
