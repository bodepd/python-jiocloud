import sys
import argparse
import time
import json
import re
import os.path
from orchestrate import DeploymentOrchestrator

#
# config file loaded from /etc/jiocloud-upgrade.json
#
# {
#    'rolling-rules': {
#        'global': '1'
#    },
#     'group-mappings': {
#         'ceph': ['st.*'],
#         'cp': ['g?cp.*']
#     },
#     'role_dependencies': {
#         'cp': ['ocdb', 'oc']
#     }
# }

class DeploymentUpgrader(DeploymentOrchestrator):

    def __init__(
        self,
        host='127.0.0.1',
        port=8500,
        key='config_state/enable_puppet',
        filename='/etc/jiocloud-upgrade.json'
    ):
        super(DeploymentUpgrader, self).__init__(host, port)
        self.key = key
        self.config_data = self.load_config(filename)
        self.validate_instructions(self.config_data)

    def load_config(self, filename):
        """
        load the config file if it exists
        """
        if os.path.isfile(filename):
            with open(filename, 'r') as fp:
                return json.load(fp)
        else:
            return {}

    # loop until an upgrade completed successfully
    def upgrade(
        self,
        version,
        instruction_data=None,
        filename=None,
        noop=False,
        verbose=False,
        retry=False,
        no_upgrade_till_done=False,
    ):
        self.validate_instructions(instruction_data)
        cv = self.current_version()
        if cv != version:
            # we are just getting started
            # 1. disable all hosts
            print 'disabling all hosts'
            self.global_disable_puppet()
            # 2. trigger an update
            self.trigger_update(version)
        # while upgrade is not complete
        while (True):
            # TODO add logic here that could only attempt the upgrade
            # if no nodes are in the upgrading state
            status = self.key_status_off_role(self.upgrade_status())
            if verbose:
                print status
            if len(status['pending']) == 0 and len(status['upgrading']) == 0:
                print "How hosts left to upgrade"
                break
            if not (len(status['upgrading']) > 0 and no_upgrade_till_done):
                upgrade_rules = self.upgrade_list(instruction_data, status)
                if verbose:
                    print upgrade_rules
                if not noop:
                    operations = self.upgrade_from_data(upgrade_rules)
                    if verbose:
                        print operations
            else:
                print "Waiting until no hosts are in upgrading state before proceeding"
            if retry:
                time.sleep(15)
            else:
                break

    # take a set of host_data and a list of instructions
    # return the set of hosts that can upgrade next
    # instructions are of the form:
    #   role_dependencies: {role:role}
    #       rules about what roles depend on what other roles
    #   rolling_rules: {global: N, role_r: N, host: N}
    #       rules of how many should be applied at once, either globally,
    #       or for any role
    #   group_mappings: {ceph: [st.*]}
    #       allows multiple roles to act like they are the same role based on
    #       the specified regex
    # at the moment, these rules are applied in the following order:
    #   1. iterate through all things listed as first, apply rolling rules for
    #      updates
    #   2. apply order rules to figure out the order in which roles are applied
    #   3. for each role that is applied, roll it out as specified by rolling
    #      rules
    #
    # TODO: there are some pretty serious locking issues here, this method
    #       cannot run more than once at the same time
    #
    def upgrade_list(self, instructions, status):
        updates = {'roles': [], 'hosts': [], 'delete_hosts': []}
        upgrading = status['upgrading']
        rolling_rules = instructions.get('rolling_rules') or self.config_data.get('rolling_rules')
        group_data = instructions.get('group_mappings') or self.config_data.get('group_mappings')
        role_order = instructions.get('role_dependencies') or self.config_data.get('role_dependencies')
        pending_subrole_mappings = self.subrole_mappings(status['pending'], group_data)
        upgrading_subrole_mappings = self.subrole_mappings(status['upgrading'], group_data)
        roles_not_allowed = self.roles_not_allowed(
            status['upgrading'],
            status['pending'],
            role_order,
            pending_subrole_mappings,
            upgrading_subrole_mappings
        )
        role_rules = rolling_rules and rolling_rules.get('roles')
        global_num = rolling_rules and rolling_rules.get('global')
        # print "Global number is: %s" % global_num
        # iterate through all things that are pending
        for role, subrole_hash in pending_subrole_mappings.iteritems():
            # if the role is not allowed, then skip it
            # what about subroles?
            if role in roles_not_allowed:
                continue
            pending_hosts = sum(subrole_hash.values(), [])
            upgrading_num = 0
            if upgrading_subrole_mappings.get(role):
                upgrading_num = len(
                    sum(upgrading_subrole_mappings.get(role).values(), [])
                )
            subroles = subrole_hash.keys()
            # TODO - hosts for subroles are not properly getting deleted
            # b/c we are just tracking the list of hosts to know when
            # and entire group of roles has completed
            num = role_rules and role_rules.get(role) or global_num
            if num is not None:
                num = int(num)
            if num is None or num >= len(pending_hosts) + upgrading_num:
                # print "%s %s %s" % (num, pending_hosts, upgrading_num)
                # if there is no rolling num, or num is greater than all
                # pending and upgrading hosts, upgrade the entire role
                # (ie: all subroles)
                for r in subroles:
                    updates['roles'].append(r)
                # get all hosts of the specified role, and mark them as
                # requirig deletion
                # TODO I am not 100% sure on this, the idea is that we should delete all of the
                # hosts keys that are set if we are upgrading the roles b/c those rules might
                # conflict with the operation that has been decided upon, I am struggling a little
                # bit to imagine all of the use cases related to this to determine if this might
                # not meet a users expectations (ie: by deleting keys that they had intended to
                # use to block a certain machine from running, I think the reality is that we
                # need to differentiate between operational pauses (which should not be overridden
                # and pauses that occur as a part of an upgrade procedure)
                for role in subroles:
                    # this deletes all hosts for all roles?
                    updates['delete_hosts'] += (status['upgrading'].get(role) or []) +\
                                               (status['upgraded'].get(role) or []) +\
                                               (status['pending'].get(role) or [])
            elif upgrading_num < num:
                num_hosts = num - upgrading_num
                # append the first N num_hosts, sort to reduce race conditions
                # print "%s %s" % (updates['hosts'], sorted(hosts)[:num_hosts])
                updates['hosts'] += sorted(pending_hosts)[:num_hosts]
            else:
                print "No action to perform, %s of %s already upgrading" % (upgrading_num, num)
        return updates

    def update_upgrade_key(self, rule, name):
        url = "/%s/%s/%s" % (self.key, rule, name)
        self.consul.kv.set(url, True)
        return url

    def delete_host_key(self, name):
        url = "/%s/host/%s" % (self.key, name)
        self.consul.kv.__delitem__(url)
        return url

    def upgrade_from_data(self, data):
        """
        Takes data that reprents all updates that need to be made
        and actually makes those updates in consul
        """
        updates = {'set': [], 'delete': []}
        for host in data['hosts']:
            updates['set'].append(self.update_upgrade_key('host', host))
        for role in data['roles']:
            updates['set'].append(self.update_upgrade_key('role', role))
        for host in data['delete_hosts']:
            updates['delete'].append(self.delete_host_key(host))
        return updates

    def global_disable_puppet(self):
        """
        Resets the state of all orchestration control keys and ensures that
        all nodes are in the pending state.
        """
        url = "%s/?recurse" % self.key
        self.consul.kv.__delitem__(url)
        self.consul.kv.set("%s/global" % self.key, False)

    def lookup_ordered_data_from_hash(self, keytype, hostnames=[], data={}):
        hosts_dict = {}
        for host in hostnames:
            hosts_dict[host] = None
            order = self.get_lookup_hash_from_hostname(host)
            for x in order:
                url = "%s/%s%s" % (keytype, x[0], x[1])
                result = data.get(url)
    #             print url
                if result is not None:
                    hosts_dict[host] = result
        return hosts_dict

    #
    #  track the progress of an upgrade:
    #    what is the current version?
    #    what hosts are set to that current version?
    #    what hosts are currently upgrading?
    #    what hosts have not even been signaled to upgrade?
    #        this might be a little slow atm, but it's ok for now
    def upgrade_status(self):
        """
        Tracks upgrade status across your fleet by splitting your nodes
        into the following categories:
        - upgraded: hosts whose version matches the current version
        - upgrading: hosts who are allowed to upgrade, but have not completed
        - pending: hosts who are not allowed to upgrade based on current orchestration
          roles (as determined by the state of the keys in consul)
        """
        results = {}
        cv = self.current_version()
        hosts = self.hosts_at_versions()
        if len(hosts) > 2:
            print "Warning: more than 2 versions, this is weird..."
        # gets hosts that have upgraded
        results['upgraded'] = hosts.pop(cv) if hosts.get(cv) else []
        # get all hosts that are not upgraded, regardless of what their current version is
        other_hosts = reduce(lambda x, y: x.union(y), hosts.values(), set())
        # lookup all data from our orchestration control key
        data = self.consul.kv.find(self.key)
        # use that data to look up the orchestration state for all hosts
        res = self.lookup_ordered_data_from_hash(self.key, other_hosts, data)
        # print res
        results['upgrading'] = []
        results['pending'] = []
        for host, value in res.iteritems():
            # assumes that falsy values mean that it is not upgrading
            # this might need to be changed to support other operaton
            # types (like it might need a version)
            if value is False or value is 'False':
                results['pending'].append(host)
            else:
                results['upgrading'].append(host)
        return results

    def key_status_off_role(self, status):
        """
        Take regular state results and index the based on role.
        converts things like:
            pending: [compute1, compute2] to
            pending: {compute:[compute1, compute2]}
        """
        status_hash = {}
        for status_type, hosts in status.iteritems():
            status_hash[status_type] = {}
            for h in hosts:
                m = self.get_host_match(h)
                if m is None:
                    raise ValueError("invalid hostname: %s" % h)
                role = m.group(1)
                if status_hash[status_type].get(role) is None:
                    status_hash[status_type][role] = []
                status_hash[status_type][role].append(h)
        return status_hash

    def subrole_mappings(self, role_data, group_data):
        """
          Checks all roles against all group mappings to create a new
          role hash of the form
            role -> subrole -> hosts
        """
        role_group_mapping = {}
        # iterate through each known role
        for role, hosts in role_data.iteritems():
            # check to see if it matches a group, use it's own role
            # name as group if nothing matches
            group_name = self.group_from_role(role, group_data)
            if role_group_mapping.get(group_name) is None:
                role_group_mapping[group_name] = {}
            role_group_mapping[group_name][role] = hosts
        return role_group_mapping

    # given group mappings a role, see if it matches a group
    # returns the matching group or the role name
    def group_from_role(self, role, group_data):
        for group_name in (group_data or []):
            for regex in group_data[group_name]:
                # print "%s %s" % (regex, role)
                if re.compile(regex).match(role):
                    return group_name
        return role

    def add_groups_to_role_list(self, mappings, roles):
        """
        take a list of roles and group role mappings, return
        a list of those rules with any groups that match those
        roles appended.
        """
        roles_and_groups = set(roles)
        new_mappings = {}
        for group, role_to_host in mappings.iteritems():
            for role in role_to_host.keys():
                if new_mappings.get(role):
                    print "role: %r has more than one subgroup" % role
                new_mappings[role] = group
        for r in roles:
            group = new_mappings.get(r)
            if group is not None:
                roles_and_groups.add(group)
        return roles_and_groups

    def roles_not_allowed(
        self,
        upgrading_roles,
        pending_roles,
        role_deps,
        pending_subrole_mappings={},
        upgrading_subrole_mappings={},
    ):
        """
        Given nodes in the current upgrading and pending state along with a hash
        or role dependencies, returns a set of nodes that cannot be upgraded.
        arguments:
          - upgrading_roles - all roles currently upgrading
          - pending_roles - all roles that are in pending state
          - role_deps - hash of each role to roles that it depends on.
          - pending_subrole_mappings
          - upgrading_subrole_mappings
          Ex:
            {compute: controller}
            would indicate that things of the compute role cannot be processed
            before the controller role.
        """
        pending_roles_and_groups = self.add_groups_to_role_list(pending_subrole_mappings, pending_roles)
        upgrading_roles_and_groups = self.add_groups_to_role_list(upgrading_subrole_mappings, upgrading_roles)
        return_data = set()
        for role in pending_roles_and_groups:
            # for each out of the roles that can still be upgraded
            if role_deps is not None:
                role_dep_list = role_deps.get(role)
                # convert strings in arrays if that is what
                # the users supplied
                if not isinstance(role_dep_list, list):
                    role_dep_list = [role_dep_list]
                for role_dep in role_dep_list:
                    # if a role has dependencies defined and those dependencies
                    # have not been completed
                    if role_dep in upgrading_roles_and_groups or role_dep in pending_roles_and_groups:
                        return_data.add(role)
                        break
        return return_data

    def validate_instructions(self, instructions):
        """
        Code to validate instructions passed in. Currently just checks that
        instruction keys are valid
        """
        allowed_keys = ['rolling_rules', 'group_mappings', 'role_dependencies']
        if instructions is not None:
            for k,v in instructions.iteritems():
                if k not in allowed_keys:
                    raise ValueError("unexpected instruction key %s" % k)

def main(argv=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='Utility for '
                                                 'performing upgrades')
    parser.add_argument('--host', type=str,
                        default='127.0.0.1', help="local consul agent")
    parser.add_argument('--port', type=int, default=8500, help="consul port")
    subparsers = parser.add_subparsers(dest='subcmd')
    upgrade_parser = subparsers.add_parser('upgrade',
                                           help='Trigger an upgrade')
    upgrade_parser.add_argument('version', type=str, help='Version to upgrade to')
    upgrade_parser.add_argument('--noop', action='store_true', help='if operation should update keys')
    upgrade_parser.add_argument('--verbose', '-v', action='store_true', help='Be verbose')
    upgrade_parser.add_argument('--retry', '-r', action='store_true', help='Retry until upgrade is complete')
    upgrade_parser.add_argument(
        '--instructions',
        type=json.loads,
        help='json rules to pass for rolling upgrades, supports keys for rolling_rules and roles',
        default={}
    )

    global_disable_parser = subparsers.add_parser('global_disable_puppet', help='Disable puppet, removes all role and host keys to ensure that all hosts will have puppet disabled')

    upgrade_status = subparsers.add_parser('status', help='show the current status of an upgrade')

    args = parser.parse_args(argv)

    du = DeploymentUpgrader(args.host, args.port)

    if args.subcmd == 'upgrade':
        du.upgrade(args.version, args.instructions, None, args.noop, args.verbose, args.retry)
    elif args.subcmd == 'global_disable_puppet':
        print du.global_disable_puppet()
    elif args.subcmd == 'status':
        print du.upgrade_status()
    else:
        print "Unexpected subcommand: %s" % args.subcmd

if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
