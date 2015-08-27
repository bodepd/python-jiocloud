# Intro

python-jiocloud contains several python libraries and command line tools
that are used for provisioning of environments as well as updates/upgrades.

## Upgrades

All upgrade actions are contained in the jiocloud.upgrade module.

NOTE: this means that if you want control over upgrades (ie:
operations other than, update everything at the same time), then
you need to use this module instead of `jorc trigger_update`.

This code reads and writes keys into consul and assumes that each
host reads those keys to determine what actions related to upgrade
they currently need to perform. (this code will be added to
maybe-upgrade.sh)

The upgrade process uses the following keys:

* /current\_version - indicates the current version that all nodes should be
  trending towards.
* /running/versions/<version>/<host> - stores each version that a host has
  successfully updated to.
* /config\_state/enable\_puppet/(global,role,host) - keys that are used to
  tell each node if they should be running puppet (which indicates an
  upgrading or upgraded state) or not (which indicates a pending state)

### Should a given host upgrade?

Each host performs logic based on the `/config\_state/enable\_puppet` directory
to determine if they should run Puppet, they do this by performing the following lookup:

* /config\_state/enable\_puppet/host/<hostname> - Check to see if a value is
  set for this key. If a value is assigned, use that to determine if we should
  perform the update.
* /config\_state/enable\_puppet/role/<rolename> - If no host key is set, see if
  a key was set for our role
* /config\_state/enable\_puppet/global - If neither a host nor a role key was,
  lookup data from the global key.

NOTE: this key structure is subject to change, and may soon change from a
boolean to a version string once we can specify version that point to repos.

### status

The status of the current upgrade can be tracked with the following command:

    python -m jiocloud.upgrade status

This command returns a hash with the keys specfied below. Each key maps to
the list of hosts currently in that state.

* upgraded - hosts who are running the current version
* upgrading - hosts where an upgrade us currently running
* pending - hosts that are not elegible to begin an upgrade

### global\_disable\_puppet

Completely wipes out all keys of the form /config\_state/enable\_puppet and
sets:

    `/config\_state/enable\_puppet/global` to False

putting all hosts whose running version does not match current|version into a
state of `pending`.

### upgrade

A special command exists to perform upgrades, this command takes a few parameters:

* version - version that should be set for current\_version
* instructions - instrcutions for how to coordinate when hosts can upgrade

*Version* is used to set current version, if it does not match the current version,
it also runs global\_puppet\_disable to put everything in the default pending state.

*Instructions* - json that can be passed in from the command line or read from the config
file */etc/jio-upgrade.json*

Instructions contain the following keys, each of these keys is used to decide
which hosts can be moved from the pendig tp upgrading states:

#### rolling\_rules

List of rules that specify how many of each role can be in the upgrading state
at a given time.

These roles can either be set globally or per role

    {
      "rolling_rules": {
        "global": 1,
        "roles": {
          "compute": 3
        }
      }
    }

The above rolling rules specify that by default, only one of each role
can be in the upgrading state at a given time, except the compute role
which can have 3 hosts in the upgrading state at a time.

NOTE: if you set rolling\_rules to '0', this will prevent a role
(or all roles from updating), this can be used to isolate rollouts
to only a subset of nodes.

#### role\_order

Specified that a role is dependent on another role or a list of
roles.

    {
      "role_dependencies": {
        "compute": "controller"
      }
    }

The above example specifies that no compute hosts will be upgraded until
all controllers have been upgraded.

#### group\_mappings

Specifies regex rules for how certain roles need to actually be of another role:

imagine the following case where there are three types of roles that are all
ceph mons: stmonleader, stmon, stmonwithosd. it may make sense to want to
apply rolling rules that will treat all of the above roles as the same role:

    {
      "group_mappings":{
        "foo":"bar"
      }
    }

NOTE: I am not sure that I like this feature, it is really complicated
and effects overall usability by making it less then obvious what rules
a given host will map to. I am considering elimating it, it should only
be considered as experimental at this state, b/c I am not sure that
the code handles all of the corner cases yet.

### performing an upgrade

There are two ways to upgrade:

1. run the upgrade script periodically by hand to track the state of your upgrade
(this is recommended until we get a little more confidence in our tooling)
2. run with --retry in which case it will keep reapplying the upgrade rules until
everything has upgraded

## next steps

still a lot more steps to perform:

1. allow arbitary commands to be encoded into the system and executed
by the fleet using the above specified upgrade rules.
2. perform upgrades based on repository versions
3. have upgrades process as 'run this set and wait until all are completed'
