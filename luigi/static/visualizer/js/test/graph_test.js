module("graph.js");

test("nodeFromTask", function() {
    var task = {
        deps: ["B","C"],
        taskId: "A",
        status: "DONE"
    };
    var expected = {
        taskId: "A",
        status: "DONE",
        trackingUrl: "#A",
        deps: ["B","C"],
        depth: -1
    };
    deepEqual(Graph.testableMethods.nodeFromTask(task), expected);
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
