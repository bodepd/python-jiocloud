import errno
import mock
import consulate
import unittest
import json
from contextlib import nested
from jiocloud.upgrade import DeploymentUpgrader

class UpgradeTests(unittest.TestCase):

    def setUp(self, *args, **kwargs):
        super(UpgradeTests, self).setUp(*args, **kwargs)
        self.do = DeploymentUpgrader('somehost', 10000)

    def test_lookup_ordered_data_from_hash(self):
        self.assertEquals(self.do.lookup_ordered_data_from_hash('config_version/enable_puppet', ['host1'], {'config_version/enable_puppet/host/host1': True}), {'host1': True})

    def test_upgrade(self):
        instructions = {'rollin_rules': ''}
        self.assertRaises(ValueError, self.do.upgrade, '123', instructions)

    def test_upgrade_list(self):
        # when there is no data, return no data
        status = {
            'upgraded': {},
            'upgrading': {},
            'pending': {}
        }
        instructions = {}
        self.assertEquals(self.do.upgrade_list(instructions, status), {
            'roles': [],
            'hosts': [],
            'delete_hosts': []
        })
        # when there are no instructions, upgrade everyone
        status = {
            'upgraded': {'h': ['h1', 'h2']},
            'upgrading': {'h': ['h3'], 'i': ['i1']},
            'pending': {'i': ['i2'], 'j': ['j1']}
        }
        instructions = {}
        self.assertEquals(self.do.upgrade_list(instructions, status), {
            'hosts': [],
            'roles': ['i', 'j'],
            'delete_hosts': ['i1', 'i2', 'j1']
        })
        # if the total number to be upgraded is higher than the upgrading and pending, upgrade all
        status = {
            'upgraded': {'h': ['h1', 'h2']},
            'upgrading': {'h': ['h3']},
            'pending': {'h': ['h4', 'h5']}
        }
        instructions = {'rolling_rules': {'global': 5}}
        self.assertEquals(self.do.upgrade_list(instructions, status), {
            'hosts': [],
            'roles': ['h'],
            'delete_hosts': ['h3', 'h1', 'h2', 'h4', 'h5']
        })
        # when upgading is greater of equal to number, do nothing
        status = {
            'upgraded': {'h': ['h1', 'h2']},
            'upgrading': {'h': ['h1', 'h2', 'h3']},
            'pending': {'h': ['h4', 'h5']}
        }
        instructions = {'rolling_rules': {'global': 3}}
        self.assertEquals(self.do.upgrade_list(instructions, status), {
            'hosts': [],
            'roles': [],
            'delete_hosts': []
        })
        # if upgrading and upgrading + pending are less then num, upgrade some hosts
        status = {
            'upgraded': {'h': ['h1', 'h2']},
            'upgrading': {'h': ['h1', 'h2', 'h3']},
            'pending': {'h': ['h4', 'h5', 'h6', 'h7']}
        }
        instructions = {'rolling_rules': {'global': 5}}
        self.assertEquals(self.do.upgrade_list(instructions, status), {
            'hosts': ['h4', 'h5'],
            'roles': [],
            'delete_hosts': []
        })
        # with global and host specific overrides
        status = {
            'upgraded': {'h': ['h1', 'h2']},
            'upgrading': {'h': ['h1'], 'i': ['i1']},
            'pending': {'h': ['h2', 'h3', 'h4'], 'i': ['i2', 'i3']}
        }
        instructions = {'rolling_rules': {'global': 3, 'roles': {'i': 2}}, }
        self.assertEquals(self.do.upgrade_list(instructions, status), {
            'hosts': ['i2', 'h2', 'h3'],
            'roles': [],
            'delete_hosts': []
        })
        # with one host in pending state
        status = {
            'upgraded': {},
            'upgrading': {},
            'pending': {'h': ['h2']}
        }
        instructions = {'rolling_rules': {'global': 1}}
        self.assertEquals(self.do.upgrade_list(instructions, status), {
            'hosts': [],
            'roles': ['h'],
            'delete_hosts': ['h2']
        })
        # when a group is marked as a dep, any role in that group should prevent us
        # from running
        self.verify_upgrade_list(
            {}, {'bar': ['bar1']}, {'baz': ['baz1']},
            {'global': 1}, {'foo': ['bar']}, {'baz': 'foo'}
        )
        # when a role in a dep group is pending, do not run a role
        self.verify_upgrade_list(
            {}, {}, {'bar': ['bar1'], 'baz': ['baz1']},
            {'global': 1}, {'foo': ['bar']}, {'baz': 'foo'},
            {'roles': ['bar'], 'hosts': [], 'delete_hosts': ['bar1']}
        )
        # when multiple roles from our group are pending, only one will upgrade
        self.verify_upgrade_list(
            {}, {}, {'bar': ['bar1'], 'baz': ['baz1']},
            {'global': 1}, {'baz': ['bar']}, {},
            # these results are actually a bug. when we finish up a subrole,
            # we should delete that subroles hosts and add it's role
            {'roles': [], 'hosts': ['bar1'], 'delete_hosts': []}
        )
        # when a role from our group is upgrading, we will wait
        self.verify_upgrade_list(
            {}, {'bar': ['bar1']}, {'baz': ['baz1']},
            {'global': 1}, {'baz': ['bar']}, {}
        )

    def verify_upgrade_list(
      self,
      upgraded,
      upgrading,
      pending,
      rolling_rules = {},
      group_mappings = {},
      role_order = {},
      result     = {
          'hosts': [],
          'roles': [],
          'delete_hosts': []
      }
    ):
        status = {
            'upgraded': upgraded,
            'upgrading': upgrading,
            'pending': pending
        }
        instructions = {
            'rolling_rules': rolling_rules,
            'group_mappings': group_mappings,
            'role_dependencies': role_order
        }
        self.assertEquals(self.do.upgrade_list(instructions, status), result)

    def test_upgrade_from_data(self):
        def consul_set_side_effect(*args, **kwargs):
            return None

        def consul_delete_side_effect(*args, **kwargs):
            return None

        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            data = {'hosts': [], 'roles': [], 'delete_hosts': []}
            consul.return_value.kv.set.side_effect = consul_set_side_effect()
            consul.return_value.kv.__delitem__.side_effect = consul_delete_side_effect()
            self.assertEquals(
                self.do.upgrade_from_data(data),
                {'set': [], 'delete': []}
            )
            data = {
                'hosts': ['h1'],
                'roles': ['h'],
                'delete_hosts': ['h2']
            }
            self.assertEquals(
                self.do.upgrade_from_data(data),
                {
                    'delete': ['/config_state/enable_puppet/host/h2'],
                    'set': ['/config_state/enable_puppet/host/h1', '/config_state/enable_puppet/role/h']
                }
            )

    def test_upgrade_status(self):
        with nested(
                mock.patch.object(self.do, 'current_version'),
                mock.patch.object(self.do, 'hosts_at_versions'),
                mock.patch.object(self.do, '_consul')
                ) as (current_version, hosts_at_versions, consul):
            current_version.return_value = '1'
            # when there are no hosts
            hosts_at_versions.return_value = {}
            consul.kv.find.return_value = {}
            self.assertEquals(self.do.upgrade_status(), {'pending': [], 'upgraded': [], 'upgrading': []})
            # when there are hosts but no config rules
            hosts_at_versions.return_value = {'1': ['h1'], '2': ['h2']}
            consul.kv.find.return_value = {}
            self.assertEquals(
                self.do.upgrade_status(),
                {'pending': [], 'upgraded': ['h1'], 'upgrading': ['h2']}
            )
            # when nothing is in the pending state
            hosts_at_versions.return_value = {'1': ['h1'], '2': ['h2', 'i3'], '3': ['i4']}
            config_state = {
                'config_state/enable_puppet/global': False,
                'config_state/enable_puppet/role/h': True,
                'config_state/enable_puppet/host/i4': 'True',
            }
            consul.kv.find.return_value = config_state
            self.assertEquals(self.do.upgrade_status(), {
                              'upgraded': ['h1'],
                              'upgrading': ['h2', 'i4'],
                              'pending': ['i3']
                              })

    def test_key_status_off_role(self):
        self.assertEquals(self.do.key_status_off_role({}), {})
        self.assertEquals(self.do.key_status_off_role(
            {'a': []}),
            {'a': {}}
        )
        self.assertEquals(self.do.key_status_off_role(
            {'a': ['h1', 'h2', 'i1']}),
            {'a': {'h': ['h1', 'h2'], 'i': ['i1']}}
        )

    def test_subrole_mappings(self):
        # with no instructions
        self.assertEquals(self.do.subrole_mappings(
            {'foo': ['foo1', 'foo2']}, {}),
            {'foo': {'foo': ['foo1', 'foo2']}}
        )
        # with a match
        self.assertEquals(self.do.subrole_mappings(
            {'foo': ['foo1', 'foo2']},
            {'bar': ['blah', 'fo.*?']}),
            {'bar': {'foo': ['foo1', 'foo2']}}
        )
        self.assertEquals(self.do.subrole_mappings(
            {'foo': ['foo1', 'foo2'], 'bar': ['bar1']},
            {'bar': ['blah', 'fo.*?']}),
            {'bar': {'bar': ['bar1'], 'foo': ['foo1', 'foo2']}}
        )

    def test_group_from_role(self):
        self.assertEquals(self.do.group_from_role(None, None), None)
        self.assertEquals(self.do.group_from_role('role', {'other_role': ['foo?']}), 'role')
        self.assertEquals(self.do.group_from_role('role', {'other_role': ['ro?']}), 'other_role')

    def test_roles_not_allowed(self):
        # non-matching roles return themselved
        self.assertEquals(
            self.do.roles_not_allowed({'a', 'b'}, {'b', 'c'}, {}),
            set()
        )
        role_order = {
            'c': 'a'
        }
        self.assertEquals(
            self.do.roles_not_allowed({'a', 'b'}, {'b', 'c'}, role_order),
            set('c')
        )
