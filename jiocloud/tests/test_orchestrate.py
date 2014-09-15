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
import etcd
import mock
import unittest
from contextlib import nested
from jiocloud.orchestrate import DeploymentOrchestrator

class OrchestrateTests(unittest.TestCase):
    def setUp(self, *args, **kwargs):
        super(OrchestrateTests, self).setUp(*args, **kwargs)
        self.do = DeploymentOrchestrator('somehost', 10000, 'disctoken')

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

    def test_new_discovery_token(self):
        with mock.patch('urllib3.PoolManager') as pm:
            token = '21476128346123'
            token_url = 'http://discovery.example.com/%s' % (token,)
            pm.return_value.request.return_value.data = token_url

            self.assertEquals(self.do.new_discovery_token('http://example.com/'), token)

            pm.return_value.request.assert_called_with('GET', 'http://example.com/new')

    def test_check_single_version(self):
        with mock.patch.object(self.do, 'running_versions') as rv:
            rv.return_value = ['123', '124']
            self.assertFalse(self.do.check_single_version('123'))

            rv.return_value = ['124']
            self.assertFalse(self.do.check_single_version('123'))

            rv.return_value = []
            self.assertFalse(self.do.check_single_version('123'))

            rv.return_value = ['123']
            self.assertTrue(self.do.check_single_version('123'))

    def test_verify_hosts(self):
        with mock.patch.object(self.do, 'hosts_at_version') as hav:
            hav.return_value = set(['cp1', 'ctrl1', 'st1'])
            self.assertTrue(self.do.verify_hosts('', ['cp1', 'ctrl1', 'st1']))
            self.assertTrue(self.do.verify_hosts('', ['cp1', 'ctrl1']))
            self.assertFalse(self.do.verify_hosts('', ['cp2', 'ctrl1']))

    def test_hosts_at_version_none(self):
        with mock.patch.object(self.do, '_etcd') as etcd:
            etcd.read.side_effect = KeyError

            self.assertEquals(self.do.hosts_at_version('foo'), [])

    class EtcdKey(object):
        def __init__(self, key, value=None):
            self.key = key
            self.value = value

    class EtcdResult(object):
        def __init__(self, children):
            self._children = children

        @property
        def children(self):
            return iter(self._children)

    def test_hosts_at_version_none_but_dir_exists(self):
        with mock.patch.object(self.do, '_etcd') as etcd:
            children = [self.EtcdKey('/running_version/foo')]
            etcd.read.return_value = self.EtcdResult(children)

            self.assertEquals(self.do.hosts_at_version('foo'), [])

    def test_hosts_at_version(self):
        with mock.patch.object(self.do, '_etcd') as etcd:
            children = [self.EtcdKey('/running_version/foo/node1'),
                        self.EtcdKey('/running_version/foo/node2')]
            etcd.read.return_value = self.EtcdResult(children)

            self.assertEquals(self.do.hosts_at_version('foo'), set(['node1',
                                                                    'node2']))

    def test_running_versions(self):
        with mock.patch.object(self.do, '_etcd') as etcd:
            children = [self.EtcdKey('/running_version/v10'),
                        self.EtcdKey('/running_version/v17'),
                        self.EtcdKey('/running_version/v18')]
            etcd.read.return_value = self.EtcdResult(children)

            self.assertEquals(self.do.running_versions(),
                              ['v10', 'v17', 'v18'])

    def test_running_versions_none(self):
        with mock.patch.object(self.do, '_etcd') as etcd:
            children = [self.EtcdKey('/running_version')]
            etcd.read.return_value = self.EtcdResult(children)

            self.assertEquals(self.do.running_versions(), [])

    def test_running_versions_none2(self):
        with mock.patch.object(self.do, '_etcd') as etcd:
            etcd.read.side_effect = KeyError

            self.assertEquals(self.do.running_versions(), [])

    def test_update_own_status(self):
        test_input = {
            'puppet': {'failed': [1,4,6,'1'],
                       'success': [0, 2],
                       'pending': [-1]},
            'validation': {'failed': [1, '1'],
                           'success': [0]}
        }
        opposite_result = {
            'puppet': {
                'failed': ['success', 'pending'],
                'success': ['failed', 'pending'],
                'pending': ['success', 'failed']
            },
            'validation': {'failed': ['success'], 'success': ['failed']}
        }
        for status_type,value in test_input.items():
            for status,status_results in value.items():
                for status_result in status_results:
                    with nested(mock.patch.object(self.do, '_etcd'),
                                mock.patch('time.time')) as (etcd, time):
                        time.return_value = 12345678

                        self.do.update_own_status(hostname='testhost',
                                                  status_type=status_type,
                                                  status_result=status_result)

                        expected_write_calls = [mock.call('/status/%s/%s/testhost' %
                                                          (status_type, status),
                                                          '12345678')]
                        expected_delete_calls = []
                        for i in opposite_result[status_type][status]:
                            expected_delete_calls.append(mock.call('/status/%s/%s/testhost' %
                                                                   (status_type, i)))

                        self.assertEquals(etcd.write.call_args_list, expected_write_calls)
                        self.assertEquals(etcd.delete.call_args_list, expected_delete_calls)

    def test_update_own_info(self):
        with nested(mock.patch.object(self.do, '_etcd'),
                    mock.patch('time.time')) as (etcd, time):
            time.return_value = 12345678

            self.do.update_own_info(hostname='testhost',
                                    interval=30,
                                    version='v13')

            expected_calls = [mock.call('/running_version/v13/testhost',
                                        '12345678'),
                              mock.call('/running_version/v13',
                                        None, dir=True,
                                        prevExist=True, ttl=70)]

            self.assertEquals(etcd.write.call_args_list, expected_calls)

    def test_update_own_info_no_version_noop(self):
        with nested(mock.patch.object(self.do, '_etcd'),
                    mock.patch.object(self.do, 'local_version')
                    ) as (etcd, local_version):
            local_version.return_value = None

            self.do.update_own_info(hostname='testhost', interval=30)

            self.assertEquals(etcd.write.call_args_list, [])

    def test_update_own_info_defaults_to_local_version(self):
        with nested(mock.patch.object(self.do, '_etcd'),
                    mock.patch.object(self.do, 'local_version')
                    ) as (etcd, local_version):
            local_version.return_value = 'v674'

            self.do.update_own_info(hostname='testhost', interval=30)

            self.assertEquals(etcd.write.call_args[0][0],
                              '/running_version/v674')

    def test_ping_succesful(self):
        with mock.patch.object(self.do, '_etcd') as etcd:
            etcd.machines = ['foo1', 'foo2']
            self.assertTrue(self.do.ping())

    def test_ping_empty_cluster(self):
        with mock.patch.object(self.do, '_etcd') as etcd:
            etcd.machines = []
            self.assertFalse(self.do.ping())

    def test_ping_failed_connection(self):
        with mock.patch.object(self.do, '_etcd') as _etcd:
            machines = mock.PropertyMock()
            machines.side_effect = etcd.EtcdException
            type(_etcd).machines = machines

            self.assertFalse(self.do.ping())

    def test_current_version(self):
        with mock.patch.object(self.do, '_etcd') as etcd:
            etcd.read.return_value = self.EtcdKey('/current_version',
                                                  '\nv673\n')
            self.assertEquals(self.do.current_version(), 'v673')

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
        with mock.patch.object(self.do, '_etcd') as etcd:
            self.do.trigger_update('v673')

            etcd.write.assert_called_with('/current_version', 'v673')



