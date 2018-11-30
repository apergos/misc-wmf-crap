# -*- coding: utf-8 -*-
"""
misc utils
"""


import os


MWSCRIPT = 'MWScript.php'
# SSH = '/usr/bin/ssh'
SSH = '/home/ariel/bin/sshes'


def get_mwscript_path(config):
    '''
    return the path to the multiversion script or at least where it would be
    '''
    return os.path.join(config['multiversion'], MWSCRIPT)


def get_maint_script_path(config, maint_script_basename):
    '''
    if we are using a multiversion setup, return the path to mwscript and the
    unaltered maintenance script path
    if we are not, return None for mwscript and an adjusted path for the maint
    script
    '''
    if config['multiversion']:
        mw_script_location = get_mwscript_path(config)
        maint_script_cmd = maint_script_basename
    else:
        mw_script_location = None
        maint_script_cmd = "{repo}/maintenance/{script}".format(
            repo=config['mwrepo'], script=maint_script_basename)
    return mw_script_location, maint_script_cmd


def prepend_command(base_command, prepends):
    '''
    add new parts of a command onto the front,
    doing the right thing if the base command is list or string
    '''
    if isinstance(base_command, str):
        command = ' '.join(prepends) + ' ' + base_command
    else:
        command = prepends + base_command
    return command


def quote_arg(arg, do_quote):
    '''
    if do_quote is true, we add single quotes to
    beginning and end of arg if it contains a
    | in it. If the arg is just the pipe symbol,
    it had better be meant to be an actual pipe
    and we leave it alone.
    '''
    if not do_quote or not arg or '|' not in arg or len(arg) == 1:
        return arg
    return "'" + arg + "'"


def quote_command(command, shell):
    '''
    if we are running a command in the shell, any argument
    with a pipe in the middle had better be enclosed in
    literal single quotes, so bash doesn't Do The Wrong Thing.
    if your argument already has single quotes in it? too bad,
    this is not meant to be an all purpose escaper.
    '''
    return [quote_arg(entry, shell) for entry in command]


def build_command(command_base, ssh_host=None, sudo_user=None, mwscript=None, php=None):
    '''
    given a command, add the ssh, sudo and mwscript pieces as needed
    if command is a mw script to be run by php, the path to the script
    must already be set correctly in command_base (i.e. full path for
    php or relative path for mwscript), this method will not check that
    '''
    # at this point only ssh commands get Popened with shell=True.
    # I guess we could do better about this someday but meh
    command = quote_command(command_base, ssh_host)

    if mwscript:
        command = prepend_command(command, [mwscript])
    if php:
        command = prepend_command(command, [php])
    if sudo_user:
        sudocmd = ['sudo', '-u', sudo_user]
        command = prepend_command(command, sudocmd)
    if ssh_host:
        sshcmd = [SSH, ssh_host]
        command = prepend_command(command, sshcmd)
    return command
