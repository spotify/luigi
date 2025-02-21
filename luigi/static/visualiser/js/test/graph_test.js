module("graph.js");

test("nodeFromTask", function() {
    var task = {
        deps: ["B1","C1"],
        taskId: "A1",
        status: "DONE",
        name: "A",
        params: {},
        priority: 0,
    };
    var expected = {
        taskId: "A1",
        status: "DONE",
        trackingUrl: "#A1",
        deps: ["B1","C1"],
        depth: -1,
        name: "A",
        params: {},
        priority: 0,
    };
    let graph = {
        hashBase: "#"
    }
    deepEqual(Graph.testableMethods.nodeFromTask.bind(graph)(task), expected);
});

test("uniqueIndexByProperty", function() {
    var input = [
        {a:"x", b:100},
        {a:"y", b:101},
        {a:"z", b:102}
    ];
    var expected = {
        "x": 0,
        "y": 1,
        "z": 2
    };
    deepEqual(Graph.testableMethods.uniqueIndexByProperty(input, "a"), expected);
});

test("createDependencyEdges", function() {
    var A = {taskId: "A", deps: ["B","C"]};
    var B = {taskId: "B", deps: ["D"]};
    var C = {taskId: "C", deps: []};
    var D = {taskId: "D", deps: []};
    var nodes = [A,B,C,D];
    var nodeIndex = {"A":0, "B":1, "C":2, "D":3};
    var edges = Graph.testableMethods.createDependencyEdges(nodes, nodeIndex);
    var expected = [
        {source: A, target: B},
        {source: A, target: C},
        {source: B, target: D}
    ];
    deepEqual(edges, expected);
});

test("computeDepth", function() {
    var A = {taskId: "A", deps: ["B","C"], depth:-1};
    var B = {taskId: "B", deps: ["D"], depth:-1};
    var C = {taskId: "C", deps: [], depth:-1};
    var D = {taskId: "D", deps: [], depth:-1};
    var E = {taskId: "C", deps: [], depth:-1};
    var nodes = [A,B,C,D,E];
    var nodeIndex = {"A":0, "B":1, "C":2, "D":3};
    Graph.testableMethods.computeDepth(nodes, nodeIndex);
    equal(A.depth, 0);
    equal(B.depth, 1);
    equal(C.depth, 1);
    equal(D.depth, 2);
    equal(E.depth, -1);
});

test("computeRowsSelfDeps", function () {
    var A1 = {name: "A", taskId: "A1", deps: ["A2"], depth: -1}
    var A2 = {name: "A", taskId: "A2", deps: [], depth: -1}
    var nodes = [A1, A2]
    var nodeIndex = {"A1": 0, "A2": 1}
    var rowSizes = Graph.testableMethods.computeRows(nodes, nodeIndex)
    equal(A1.depth, 0)
    equal(A2.depth, 1)
    deepEqual(rowSizes, [1, 1])
});

test("computeRowsGrouped", function() {
    var A0 = {name: "A", taskId: "A0", deps: ["D0", "B0"], depth: -1}
    var B0 = {name: "B", taskId: "B0", deps: ["C1", "C2"], depth: -1}
    var C1 = {name: "C", taskId: "C1", deps: ["D1", "E1"], depth: -1}
    var C2 = {name: "C", taskId: "C2", deps: ["D2", "E2"], depth: -1}
    var D0 = {name: "D", taskId: "D0", deps: [], depth: -1}
    var D1 = {name: "D", taskId: "D1", deps: [], depth: -1}
    var D2 = {name: "D", taskId: "D2", deps: [], depth: -1}
    var E1 = {name: "E", taskId: "E1", deps: [], depth: -1}
    var E2 = {name: "E", taskId: "E2", deps: [], depth: -1}
    var nodes = [A0, B0, C1, C2, D0, D1, D2, E1, E2]
    var nodeIndex = {"A0": 0, "B0": 1, "C1": 2, "C2": 3, "D0": 4, "D1": 5, "D2": 6, "E1": 7, "E2": 8}
    var rowSizes = Graph.testableMethods.computeRows(nodes, nodeIndex)
    equal(A0.depth, 0)
    equal(B0.depth, 1)
    equal(C1.depth, 2)
    equal(C2.depth, 2)
    equal(D0.depth, 3)
    equal(D1.depth, 3)
    equal(D2.depth, 3)
    equal(E1.depth, 4)
    equal(E2.depth, 4)
    deepEqual(rowSizes, [1, 1, 2, 3, 2])
});

test("createGraph", function() {
    var tasks = [
        {taskId: "A", deps: ["B","C"], status: "PENDING"},
        {taskId: "B", deps: ["D"], status: "RUNNING"},
        {taskId: "C", deps: [], status: "DONE"},
        {taskId: "D", deps: [], status: "DONE"},
        {taskId: "E", deps: [], status: "DONE"}
    ];
    var graph = Graph.testableMethods.createGraph(tasks);
    equal(graph.nodes.length, 4);
    equal(graph.links.length, 3);
    $.each(graph.nodes, function() {
        notEqual(this.x, 0);
        notEqual(this.y, 0);
    });

    // TODO: more assertions
});
