UNKNOWN = 'UNKNOWN'
SCHEDULING = 'SCHEDULING'
RUNNING = 'RUNNING'

_STATES = frozenset((SCHEDULING, RUNNING))
_STATE = UNKNOWN


def set_state(state):
    assert state in _STATES
    global _STATE
    _STATE = state


def get_state():
    return _STATE
