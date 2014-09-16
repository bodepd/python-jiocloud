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
import etcd
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
from urllib3.exceptions import HTTPError


class DiscoveryClient(etcd.Client):
    def __init__(self, discovery_token, *args, **kwargs):
        self.discovery_token = discovery_token
        super(DiscoveryClient, self).__init__(*args, **kwargs)

    @property
    def key_endpoint(self):
        return '/%s' % (self.discovery_token,)


class DeploymentOrchestrator(object):
    UPDATE_AVAILABLE = 0
    UP_TO_DATE = 1
    NO_CLUE = 2
    NO_CLUE_BUT_WERE_JUST_GETTING_STARTED = 3

    def __init__(self, host='127.0.0.1', port=4001, discovery_token=None):
        self.host = host
        self.port = port
        self.discovery_token = discovery_token
        self._etcd = None

    @property
    def etcd(self):
        if not self._etcd:
            if self.discovery_token:
                dc = DiscoveryClient(self.discovery_token,
                                     host='discovery.etcd.io',
                                     port=443, protocol='https',
                                     allow_redirect=False,
                                     allow_reconnect=False)
                urls = [x.value for x in dc.read('/').children]
                conn_tuples = [(urlparse.urlparse(url).netloc.split(':')[0],
                                self.port) for url in urls]
            else:
                conn_tuples = [(self.host, self.port)]

            self._etcd = etcd.Client(host=tuple(conn_tuples))
        return self._etcd

    def trigger_update(self, new_version):
        self.etcd.write('/current_version', new_version)

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
        return self.etcd.read('/current_version').value.strip()

    def ping(self):
        try:
            return bool(self.etcd.machines)
        except (etcd.EtcdError, etcd.EtcdException, HTTPError):
            return False

    # get all ip addresses for all interfaces, return a hash interface => address
    def get_ips(self):
        ips = {}
        for i_name in netifaces.interfaces():
            i_face = netifaces.ifaddresses(i_name)
            inet_num = netifaces.AF_INET
            if i_face.has_key(inet_num) and i_face[inet_num][0]['addr']:
                ips[i_name] = i_face[inet_num][0]['addr']
        return ips

    # publish addresses for all services not in our ignore list
    def publish_service(self, rolestoignore=None):
        hostname = socket.gethostname()
        m = re.search(r"([a-z]+)\d+-?", hostname)
        role = m.group(1)
        if not (rolestoignore and role in rolestoignore):
            ips = self.get_ips()
            self.etcd.write('/available_services/%s/%s' % (role, hostname), json.dumps(ips))
            return ips
        else:
            return None

    # retrieve all address information for all services, and organize into the structure:
    # 'services::<role>::interface: [list of ips]'
    def get_service_data(self):
        try:
            data = self.etcd.read('/available_services/', recursive=True)
        except KeyError:
            return False
        leaves = list(data.leaves())
        service_address_parser = {}
        hiera_data = {}
        for leaf in leaves:
            role = os.path.split(os.path.split(leaf.key)[0])[1]
            service_address_parser.setdefault(role, {})
            for interface,address in json.loads(leaf.value).iteritems():
                service_address_parser[role].setdefault(interface, [])
                service_address_parser[role][interface].append(address)
        for role,interfaces in service_address_parser.iteritems():
            for i,addr in interfaces.iteritems():
                hiera_data['services::%s::%s' % (role, i)] = addr
        return hiera_data

    def write_service_data_to_hiera(self, hiera_file='/etc/puppet/hiera/data/services.yaml'):
        hiera_data = self.get_service_data()
        try:
            with open(hiera_file, 'w') as fp:
                yaml.safe_dump(hiera_data, fp)
                return hiera_data
        except IOError, e:
            if e.errno == errno.ENOENT:
                return ''
            raise

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

        self.etcd.write('%s/%s' % (status_dir, hostname), str(time.time()))

        for delete_dir in delete_dirs:
            try:
                self.etcd.delete('%s/%s' % (delete_dir, hostname))
            except KeyError:
                return True

        return True


    def update_own_info(self, hostname, interval=60, version=None):
        version = version or self.local_version()
        if not version:
            return
        version_dir = '/running_version/%s' % version
        self.etcd.write('%s/%s' % (version_dir, hostname), str(time.time()))
        self.etcd.write(version_dir, None, dir=True,
                        prevExist=True, ttl=(interval*2+10))

    def running_versions(self):
        try:
            res = self.etcd.read('/running_version')
        except KeyError:
            return []
        return filter(lambda x: x != 'running_version',
                      [x.key.split('/')[-1] for x in res.children])

    def hosts_at_version(self, version):
        version_dir = '/running_version/%s' % (version,)
        try:
            res = self.etcd.read(version_dir)
        except KeyError:
            return []
        if len(list(res.children)) == 1:
            if list(res.children)[0].key == version_dir:
                return []
        return set([x.key.split('/')[-1] for x in res.children])

    def get_failures(self, hosts):
        try:
            val_failures = list(self.etcd.read('/status/validation/failed').leaves)
        except KeyError:
            val_failures = []
        try:
            puppet_failures = list(self.etcd.read('/status/puppet/failed').leaves)
        except KeyError:
            puppet_failures = []
        if hosts:
            for host in val_failures:
                print "Validation Failure:%s" % os.path.basename(host.key)
            for host in puppet_failures:
                print "Puppet Failure:%s" % os.path.basename(host.key)
        else:
            print "Validation Failures:%s"        % len(val_failures)
            print "Puppet Validation Failures:%s" % len(puppet_failures)
        return len(puppet_failures) == 0 and len(val_failures) == 0


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

    def new_discovery_token(self, discovery_endpoint):
        http = urllib3.PoolManager()
        r = http.request('GET', '%snew' % (discovery_endpoint,))
        return r.data.split('/')[-1]

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
                        default='127.0.0.1', help="etcd host")
    parser.add_argument('--port', type=int, default=4001, help="etcd port")
    parser.add_argument('--discovery_token', type=str,
                        default=None, help="etcd discovery token")
    subparsers = parser.add_subparsers(dest='subcmd')

    trigger_parser = subparsers.add_parser('trigger_update',
                                           help='Trigger an update')
    trigger_parser.add_argument('version', type=str, help='Version to deploy')

    current_version_parser = subparsers.add_parser('current_version',
                                                   help='Get available version')

    ping_parser = subparsers.add_parser('ping', help='Ping etcd')

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
    update_own_info_parser.add_argument('--interval', type=int, default=60, help="Update interval")
    update_own_info_parser.add_argument('--hostname', type=str, default=socket.gethostname(),
                                        help="This system's hostname")
    update_own_info_parser.add_argument('--version', type=str,
                                        help="Override version to report into etcd")

    running_versions_parser = subparsers.add_parser('running_versions', help="List currently running versions")

    verify_hosts_parser = subparsers.add_parser('verify_hosts', help="Verify that list of hosts are all available")
    verify_hosts_parser.add_argument('version', help="Version to look for")

    new_discovery_token_parser = subparsers.add_parser('new_discovery_token', help="Get new discovery token")
    new_discovery_token_parser.add_argument('--endpoint', default='https://discovery.etcd.io/', help='Discovery token endpoint')

    check_single_version_parser = subparsers.add_parser('check_single_version', help="Check if the given version is the only one currently running")
    check_single_version_parser.add_argument('version', help='The version to check for')
    check_single_version_parser.add_argument('--verbose', '-v', action='store_true', help='Be verbose')
    publish_parser = subparsers.add_parser('publish_service', help="Publish a service")
    publish_parser = subparsers.add_parser('get_services', help="Retrieve all service data")
    publish_parser = subparsers.add_parser('cache_services', help="Cache state of published services as hiera data")
    args = parser.parse_args(argv)

    do = DeploymentOrchestrator(args.host, args.port, args.discovery_token)
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
    elif args.subcmd == 'new_discovery_token':
        print do.new_discovery_token(args.endpoint)
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
    elif args.subcmd == 'publish_service':
        return do.publish_service()
    elif args.subcmd == 'get_services':
        return do.get_service_data()
    elif args.subcmd == 'cache_services':
        return do.write_service_data_to_hiera()

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
