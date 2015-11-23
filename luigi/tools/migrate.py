"""
Migrate the task_history database to the latest version.

This script provides 1-way version migration of the db_task_history database to the latest
version.

CURRENT_SOURCE_VERSION defines the version number of the schema in the source code.
The schema version on the database is stored in the table `version`

"""

from __future__ import print_function
import sys
from luigi.db_task_history import DbTaskHistory, version_table
from luigi import configuration

CURRENT_SOURCE_VERSION = 1


# ---------------------------------------------------------------------------
# Version migration functions

def version_1(session):
    """
    Add task_id column to tasks table.  Required to make a robust connection between
    task_id and a TaskRecord.

    """

    session.execute('ALTER TABLE tasks ADD COLUMN task_id VARCHAR(200)')
    session.execute('CREATE INDEX ix_task_id ON tasks (task_id)')


# version_func[db-version]() --> next version
version_funcs = {0: version_1}


# ---------------------------------------------------------------------------

def main():
    # Note : The version table will be created automatically when calling DbTaskHistory()
    task_history = DbTaskHistory()
    session = task_history.session_factory()

    config = configuration.get_config()
    connection_string = config.get('task_history', 'db_connection')

    print('Luigi db_task_history migration tool')
    db_version = get_version(session)

    if db_version == CURRENT_SOURCE_VERSION:
        print('Your schema version is up to date')
        sys.exit(0)
    elif db_version > CURRENT_SOURCE_VERSION:
        print('ERROR: Your schema version is greater than the source version ({}>{})'.format(db_version,
                                                                                             CURRENT_SOURCE_VERSION))
        sys.exit(1)

    print('Migration required.  '
          'Your schema version is less than the source version ({}<{})'.format(db_version,
                                                                               CURRENT_SOURCE_VERSION))

    print('******************************************************')
    print('** WARNING Do not proceed without a database backup.  ')
    print('******************************************************')
    print()

    if query_yes_no('Do you want to migrate database {} now?'.format(connection_string), default='no'):
        do_migration(db_version, CURRENT_SOURCE_VERSION, session)
    else:
        sys.exit(1)


# ---------------------------------------------------------------------------


def do_migration(from_version, to_version, session):
    for v in range(from_version, to_version):
        new_version = v + 1
        print('Migrating version {} -> {}'.format(v, new_version))
        version_funcs[v](session)
        set_version(new_version, session)
        session.commit()


def get_version(session):
    version_row = session.execute(version_table.select()).first()
    if version_row is None:
        session.execute(version_table.insert(values={'version': 0}))
        session.commit()

        return 0
    else:
        return version_row[0]


def set_version(version, session):
    session.execute(version_table.update(values={'version': version}))


def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    From http://code.activestate.com/recipes/577058-query-yesno/

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
    It must be "yes" (the default), "no" or None (meaning
    an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = raw_input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")
