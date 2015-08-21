#!/bin/bash
set -x

#
# This script was used during developent to perform integration tests
# on a machine where consul is running.
# Each function represents a set of actions that were crafted to capture
# the kinds of scenarios that the code was intended to handle.
#
# To run through a scenario, invoke the bash function and run run
# this script. Reveiw the results to ensure that the expected machines
# were moved from the pending to upgrading states as expected based on
# the specified rules

#
# one host from each role can be in the upgrading
# state at a time when their rolling rules are set to 1
#
test_one() {

old_version="${1-v0}"
new_version="${2-v1}"

  # just test basic upgrade one at a time rule
  instructions='{"rolling_rules":{"global":"1"}}'
  upgrade_args="--verbose --instructions=$instructions"

  # revert everything to previous version
  jorc update_own_info --hostname foo1 --version $old_version
  jorc update_own_info --hostname foo2 --version $old_version
  jorc update_own_info --hostname foo3 --version $old_version
  jorc update_own_info --hostname bar1 --version $old_version
  jorc update_own_info --hostname bar2 --version $old_version

  # clear keys out even if current version is the same
  python -m jiocloud.upgrade global_disable_puppet
  # upgrade first set of hosts
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args
  # emulate upgrade on those hosts
  jorc update_own_info --hostname foo1 --version $new_version
  jorc update_own_info --hostname bar1 --version $new_version

  # upgrade second set of hosts
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args
  # emulate upgrade on those hosts
  jorc update_own_info --hostname foo2 --version $new_version
  jorc update_own_info --hostname bar2 --version $new_version

  # perform last upgrade action
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args
  # simluate success for final host
  jorc update_own_info --hostname foo3 --version $new_version

  #verify that there is no work left to do
  python -m jiocloud.upgrade status
}

#
# only one host from foo or bar can be in the upgrading state at a time
# when they are both part of the same group
#
test_two() {

  old_version="${1-v0}"
  new_version="${2-v1}"

  # just test basic upgrade one at a time rule
  # also set a rule such that things of the bar role act like they are in the
  # foo role
  instructions='{"rolling_rules":{"global":"1"},"group_mappings":{"bar":"foo"}}'
  upgrade_args="--verbose --instructions=$instructions"
  # things of bar actually are of type foo


  # revert everything to previous version
  jorc update_own_info --hostname foo1 --version $old_version
  jorc update_own_info --hostname foo2 --version $old_version
  jorc update_own_info --hostname bar1 --version $old_version
  jorc update_own_info --hostname bar2 --version $old_version

  # clear keys out even if current version is the same
  python -m jiocloud.upgrade global_disable_puppet
  # upgrade first set of hosts
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

  jorc update_own_info --hostname bar1 --version $new_version
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

  jorc update_own_info --hostname bar2 --version $new_version
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

  jorc update_own_info --hostname foo1 --version $new_version
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

  jorc update_own_info --hostname foo2 --version $new_version
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

  jorc update_own_info --hostname foo3 --version $new_version
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

}


#
# just roll one at a time, block 2 roles that map to the same
# group on another role
#
test_three() {

  old_version="${1-v0}"
  new_version="${2-v1}"

  # just test basic upgrade one at a time rule
  # also set a rule such that things of the bar role act like they are in the
  # foo role
  instructions='{"rolling_rules":{"global":"1"},"group_mappings":{"bar":"foo"},"role_order":{"bar":"baz"}}'
  upgrade_args="--verbose --instructions=$instructions"
  # things of bar actually are of type foo


  # revert everything to previous version
  jorc update_own_info --hostname foo1 --version $old_version
  jorc update_own_info --hostname foo2 --version $old_version
  jorc update_own_info --hostname bar1 --version $old_version
  jorc update_own_info --hostname baz1 --version $old_version

  # clear keys out even if current version is the same
  python -m jiocloud.upgrade global_disable_puppet
  # upgrade first set of hosts
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

  jorc update_own_info --hostname baz1 --version $new_version
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

  jorc update_own_info --hostname bar1 --version $new_version
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

  jorc update_own_info --hostname foo1 --version $new_version
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

  jorc update_own_info --hostname foo2 --version $new_version
  python -m jiocloud.upgrade upgrade $new_version $upgrade_args

}

test_three v13 v14
