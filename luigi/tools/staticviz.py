'''generates static graph visualisation of given luigi dag

Generates a static (png, .dot, svg) graph visualisation
of given luigi dag. Useful for documentation and presentations

Variables:
    digraph -- [description]
'''
import os.path as p
import graphviz as gv
from queue import Queue
FORMATS = 'dot', 'png', 'pdf', 'svg', 'gv'


class LuigiGraph(gv.Digraph):

    def __init__(self, **kwargs):
        super().__init__(format='svg', endine='dot', **kwargs)

    def nodes(self, nodes):
        for n in nodes:
            if isinstance(n, tuple):
                self.node(n[0], **n[1])
            else:
                self.node(n)

    def _all_attr(self, style: dict):
        self.graph_attr.update(style.get('graph', {}))
        self.node_attr.update(style.get('nodes', {}))
        self.edge_attr.update(style.get('edges', {}))


def _get_required(task):
    '''returns a list of task dependencies'''
    try:
        rqs = task.requires()
        if isinstance(rqs, dict):
            return list(rqs.values())
        elif not isinstance(rqs, list):  # object
            return [rqs, ]  # convert to list
        return rqs
    except Exception:
        return []


def _get_node_name(node, omit_params=False):
    '''return node name
    if omit_params, will return only task name (without params)'''

    # TODO: durty fix, graphviz does not assept
    # colon (even escaped) in node name
    name = node.__repr__().replace(':', '')

    # NOTE: shall I move this out of get_name - more of a styling Q
    if omit_params:
        name = name.split('(')[0]
    return name


def _generate_nodes_edges(last_task, omit_params: bool = False) -> dict:
    ''''generates graph back from the last task

    NOTE: for now supports only one, last task in the graph. in the future
    will support multiple tasks as end-points

    Arguments:
        last_task (luigi.Task) -- last task to start graph from.
            Assumed to have `__name__` property and `.requires()` method

        omit_params (bool) -- if true, node name will keep only task
            name (not parameters)
    '''
    nodes, edges = [], []
    Q = Queue()
    Q.put([last_task, None])  # start with the last task and no edges

    while not Q.empty():
        node, dependent = Q.get()

        node_name = _get_node_name(node, omit_params=omit_params)
        nodes.append(node_name)

        if dependent:
            edges.append([node_name, dependent])

        # GET TASK DEPDENDENCIES
        for el in _get_required(node):
            Q.put([el, node_name])

    return {'nodes': nodes, 'edges': edges}


def _generate_graph(info: dict):
    '''build and render graph, given nodes and edges

    Arguments:
        info (dict) -- dictionary of nodes and edges
    Returns:
        graph - DAG graph representation
    '''
    graph = LuigiGraph()

    # NOTE: proof-of-concept, does not support custom node styling yet
    graph.nodes(info['nodes'])
    graph.edges(info['edges'])

    # style graph

    return graph


def _infer_format(path):
    _, ext = p.splitext(path)

    ext = ext[1:]  # remove dot
    if ext in FORMATS:
        return ext

    raise ValueError(f'Failed to infer format from {path}, supposed to be one of {formats}')


def generate_graph(task, filepath=None, format_=None, style=None, view=False, **kwargs):
    '''central method of statviz, generates a graph
    and renders it to the file

    Arguments:
        task (luigi.Task) - endpoint task of the DAG to visualise
        filepath (str) - path to the file to store render. if no
            path is given, function will return graph object instead
        format_ (str) - render format (png, svg, pdf, dot, gv). if None,
            will try to infer format from file pathname
        style (dict) - styling dictionary
        view (bool) - if true, will immidiately open the saved file
            in default viewer

    Returns:
        None or graphviz.Digraph
    '''
    omit = kwargs.get('omit_names', None)
    info = _generate_nodes_edges(task, omit_names=omit)

    graph = _generate_graph(info)

    if style:
        graph._all_attr(style)

    if filepath is None:
        return graph

    filename, frmt = p.splitex(filepath)
    if format_:
        frmt = format_
    else:
        frmt = frmt[1:]  # drop dot
    assert frmt in FORMATS, f'Format is not supported: {frmt_}'

    if frmt != 'svg':
        graph.engine = frmt

    directory = p.dirname(filepath)
    graph.render(filename=filename, directory=directory,
                 view=view, cleanup=True)

