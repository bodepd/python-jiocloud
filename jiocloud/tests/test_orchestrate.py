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
import errno
import mock
import consulate
import unittest
import json
from contextlib import nested
from jiocloud.orchestrate import DeploymentOrchestrator

class OrchestrateTests(unittest.TestCase):
    def setUp(self, *args, **kwargs):
        super(OrchestrateTests, self).setUp(*args, **kwargs)
        self.do = DeploymentOrchestrator('somehost', 10000)

    def test_local_version(self):
        open_mock = mock.mock_open(read_data='\n54\n')
        with mock.patch('__builtin__.open', open_mock):
            self.do.local_version()

            self.assertEquals(self.do.local_version(), '54')

    def test_local_version_enoent(self):
        with mock.patch('__builtin__.open') as open_mock:
            enoent = IOError()
            enoent.errno = errno.ENOENT
            open_mock.side_effect = enoent

            self.do.local_version()

            self.assertEquals(self.do.local_version(), '')

    def test_local_version_other_ioerror(self):
        with mock.patch('__builtin__.open') as open_mock:
            eperm = IOError()
            eperm.errno = errno.EPERM
            open_mock.side_effect = eperm

            self.assertRaises(IOError, self.do.local_version)

    def test_local_version_set(self):
        open_mock = mock.mock_open()
        with mock.patch('__builtin__.open', open_mock):
            version = 'abc123'
            self.assertEquals(self.do.local_version(version), version)
            open_mock().write.assert_called_with(version)

    def test_verify_hosts(self):
        with mock.patch.object(self.do, 'hosts_at_version') as hav:
            hav.return_value = set(['cp1', 'ctrl1', 'st1'])
            self.assertTrue(self.do.verify_hosts('', ['cp1', 'ctrl1', 'st1']))
            self.assertTrue(self.do.verify_hosts('', ['cp1', 'ctrl1']))
            self.assertFalse(self.do.verify_hosts('', ['cp2', 'ctrl1']))

    def test_hosts_at_version_none(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.kv.find.side_effect = KeyError

            self.assertEquals(self.do.hosts_at_version('foo'), [])

    def test_hosts_at_version_none_but_dir_exists(self):
        with mock.patch.object(self.do, '_consul') as consul:
            consul.kv.find.return_value = [
                '/running_version/foo/'
                ]
            self.assertEquals(self.do.hosts_at_version('foo'), set([]))

    def test_hosts_at_version(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.kv.find.return_value = [
                '/running_version/foo/node1',
                '/running_version/foo/node2'
                ]
            self.assertEquals(self.do.hosts_at_version('foo'), set(['node1', 'node2']))
            consul.return_value.kv.find.assert_called_with('/running_version/foo')

    def test_running_versions(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.kv.find.return_value = [
                'running_version/v10',
                'running_version/v11',
                'running_version/v12'
                ]
            self.assertEquals(self.do.running_versions(),
                              set(['v10', 'v11', 'v12']))

    def test_running_versions_none(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.kv.find.return_value = [
                'running_version',
                ]
            self.assertEquals(self.do.running_versions(), set())

    def test_running_versions_none2(self):
        with mock.patch.object(self.do, '_consul') as consul:
            consul.kv.find.side_effect = KeyError
            self.assertEquals(self.do.running_versions(), set())

    def test_get_failures_failing(self):
        #Test warnings
        def get_warnings(state):
            if state in ['warning']:
                return [{"Name":"puppet"}]
            else:
                return []
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock):
            self.do.consul.health.state.side_effect = get_warnings
            self.assertFalse(self.do.get_failures())
        #Test critical failures
        def get_critical_failures(state):
            if state in ['critical']:
                return [{"Name":"Failure"}]
            else:
                return []
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock):
            self.do.consul.health.state.side_effect = get_critical_failures
            self.assertFalse(self.do.get_failures())

    def test_get_failures_passing(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock):
            self.do.consul.health.state.return_value = []
            self.assertTrue(self.do.get_failures())


#    def test_update_own_status(self):

    def test_update_own_info(self):
        with nested(mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock),
                    mock.patch('time.time')
          ) as (consul, time):
            time.return_value = 12345678
            new_version = 'v14'
            old_version = 'v13'
            hostname = 'testhost'
            consul.return_value.kv.find.return_value = [
                'running_version/%s/%s' %(old_version, hostname),
                ]

            self.do.update_own_info(hostname=hostname,
                                    version=new_version)
            expected_calls = [mock.call('/running_version/%s/%s' %(new_version, hostname),
                                        '12345678')]
            self.assertEquals(consul.return_value.kv.set.call_args_list, expected_calls)
            expected_calls = [mock.call('running_version/%s/%s' %(old_version, hostname))]            
            self.assertEquals(consul.return_value.kv.__delitem__.call_args_list, expected_calls)

    def test_update_own_info_no_version_noop(self):
        with nested(mock.patch.object(self.do, '_consul'),
                    mock.patch.object(self.do, 'local_version')
                    ) as (consul, local_version):
            local_version.return_value = None

            self.do.update_own_info(hostname='testhost')

            self.assertEquals(consul.write.call_args_list, [])

    def test_update_own_info_defaults_to_local_version(self):
        with nested(mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock),
                    mock.patch.object(self.do, 'local_version')
          ) as (consul, local_version):
            local_version.return_value = 'v674'
            self.do.update_own_info(hostname='testhost')
            self.assertEquals(consul.return_value.kv.set.call_args[0][0],
                              '/running_version/v674/testhost')

    def test_ping_succesful(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.agent.members.return_value = ['foo1', 'foo2']
            self.assertTrue(self.do.ping())

    def test_ping_empty_cluster(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.agent.members.return_value = []
            self.assertFalse(self.do.ping())

    def test_ping_failed_connection(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.agent.members.side_effect = IOError
            self.assertFalse(self.do.ping())

    def test_current_version(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.kv.get.return_value = 'v673 '
            self.assertEquals(self.do.current_version(), 'v673')
            consul.return_value.kv.get.assert_called_with('/current_version')

    def test_manage_config(self):
        # test with bad data
        self.assertFalse(self.do.manage_config('bad_type', None, None, None))
        self.assertFalse(self.do.manage_config('state', 'bad_value', None, None))
        self.assertFalse(self.do.manage_config('state', 'role', 'data', None))
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            self.assertEquals(self.do.manage_config('config_state', 'global', 'key', 'value', None), 'value')
            consul.return_value.kv.set.assert_called_with('/config_state/global/key', 'value')

    def test_lookup_ordered_data(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.kv.find.return_value = dict({'key':'v673'})
            self.assertEquals(self.do.lookup_ordered_data('config_version', 'host1'), {'key':'v673'})
            consul.return_value.kv.find.assert_called_with('/config_version/host/host1/')
            self.assertEquals(self.do.lookup_ordered_data('config_version', 'host1', {}), {})
            self.assertEquals(self.do.lookup_ordered_data('config_version', 'host1', {'config_version/host/host1': {'key':'v673'}}), {'key': 'v673'})
            self.assertEquals(self.do.lookup_ordered_data('config_version', 'host1', {
                'config_version/host/host1':{'k1':'v1'},
                'config_version/global':{'k2':'v2'},
                'config_version/role/host':{'k3':'v3'}
                 }), {'k1':'v1', 'k2':'v2', 'k3':'v3'})

    def test_get_lookup_hash_from_hostname(self):
        self.assertEqual(self.do.get_lookup_hash_from_hostname('host12'), [['global', ''], ['role', '/host'], ['host', '/host12']])

    def test_check_puppet(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.kv.find.return_value = dict({'enable_puppet':True})
            self.assertEquals(self.do.check_puppet('node1.jiocloud.com'), 0)
            consul.return_value.kv.find.return_value = dict({'enable_puppet':False})
            self.assertEquals(self.do.check_puppet('node1.jiocloud.com'), 9)
            consul.return_value.kv.find.assert_called_with('/config_state/host/node1.jiocloud.com/')

    def test_enable_puppet(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.kv.find.return_value = dict({'enable_update':True})
            self.assertEquals(self.do.enable_puppet('True', 'global', None,'set'), 'True')
            consul.return_value.kv.set.assert_called_with('/config_state/global/enable_puppet','True')
            self.assertEquals(self.do.enable_puppet('False', 'host', 'node1.jiocloud.com','set'), 'False')
            consul.return_value.kv.set.assert_called_with('/config_state/host/node1.jiocloud.com/enable_puppet','False')
            self.assertEquals(self.do.enable_puppet('False', 'role', 'cp','set'), 'False')
            consul.return_value.kv.set.assert_called_with('/config_state/role/cp/enable_puppet','False')
            self.assertEquals(self.do.enable_puppet('False', 'role', 'cp','delete'), 'False')
            consul.return_value.kv.__delitem__.assert_called_with('/config_state/role/cp/enable_puppet')

    def test_set_config(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            consul.return_value.kv.find.return_value = dict({'enable_update':True})
            self.assertEquals(self.do.set_config('enable_update', 'False', 'host', 'node1.jiocloud.com'), 'False')
            consul.return_value.kv.set.assert_called_with('/config_state/host/node1.jiocloud.com/enable_update','False')
            self.assertEquals(self.do.set_config('enable_update', 'False', 'host', 'node1.jiocloud.com','config_state','delete'), 'False')
            consul.return_value.kv.__delitem__.assert_called_with('/config_state/host/node1.jiocloud.com/enable_update')

    def test_pending_update(self):
        with nested(
                mock.patch.object(self.do, 'local_version'),
                mock.patch.object(self.do, 'current_version')
                ) as (local_version, current_version):
            local_version.return_value = 'v123'
            current_version.return_value = 'v123'

            self.assertEquals(self.do.pending_update(),
                              self.do.UP_TO_DATE)

            local_version.return_value = ''
            current_version.return_value = 'v123'

            self.assertEquals(self.do.pending_update(),
                              self.do.UPDATE_AVAILABLE)

            local_version.return_value = 'v1234'
            current_version.return_value = 'v123'

            self.assertEquals(self.do.pending_update(),
                              self.do.UPDATE_AVAILABLE)

            local_version.return_value = 'v123'
            current_version.side_effect = KeyError

            self.assertEquals(self.do.pending_update(),
                              self.do.NO_CLUE)

            local_version.return_value = ''
            current_version.side_effect = KeyError

            self.assertEquals(self.do.pending_update(),
                              self.do.NO_CLUE_BUT_WERE_JUST_GETTING_STARTED)

    def test_trigger_update(self):
        with mock.patch('jiocloud.orchestrate.DeploymentOrchestrator.consul', new_callable=mock.PropertyMock) as consul:
            self.do.trigger_update('v673')

            consul.return_value.kv.set.assert_called_with('/current_version', 'v673')



