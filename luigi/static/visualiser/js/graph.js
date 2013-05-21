Graph = (function() {
    var statusColors = {
        "FAILED":"#DD0000",
        "DONE":"#00DD00",
        "PENDING":"#EEBB00",
        "RUNNING":"#0044DD"
    };

    function nodeFromTask(task) {
        return {
            taskId: task.taskId,
            status: task.status,
            trackingUrl: "#"+task.taskId,
            x: graphWidth/2+Math.random()*10,
            y: graphHeight/2+Math.random()*10,
            deps: task.deps
        };
    }

    /* Fix root node to a specific point on the graph */
    function pinRootNode(node) {
        node.fixed = true;
        node.x = graphWidth/2;
        node.y = 50;
    }

    /* Convert array to dict by indexing on propertyName */
    function uniqueIndexByProperty(data, propertyName) {
        var nodeIndex = {};
        $.each(data, function(i, dataPoint) {
            nodeIndex[dataPoint[propertyName]] = i;
        });
        return nodeIndex;
    }

    /* Create edges between the supplied node using the deps property of each node */
    function createDependencyEdges(nodes, nodeIndex) {
        var edges = [];
        $.each(nodes, function(i, task) {
            $.each(task.deps, function(j, dep) {
                edges.push({
                    source: nodeIndex[task.taskId],
                    target: nodeIndex[dep]
                });
            });
        });
        return edges;
    }

    /* Compute the maximum depth of each node for layout purposes (output not currently used) */
    function computeDepth(nodes, nodeIndex) {
        function descend(n, depth) {
            n.depth = depth;
            $.each(n.deps, function(i, dep) {
                descend(nodes[nodeIndex[dep]], depth + 1);
            });
        }
        descend(nodes[0], 0);
        console.log(nodes);
    }

    /* Parses a list of tasks to a graph format suitable for d3 force-directed layout */
    function parseToGraph(tasks) {
        var nodes = $.map(tasks, nodeFromTask);
        console.log(nodes);
        var nodeIndex = uniqueIndexByProperty(nodes, "taskId");
        var edges = createDependencyEdges(nodes, nodeIndex);

        if (nodes.length > 0) {
            pinRootNode(nodes[0]);
            computeDepth(nodes, nodeIndex);
        }

        return {
            nodes: nodes,
            links: edges
        };
    }

    var graphWidth = 1110;
    var graphHeight = 800;

    function DependencyGraph(containerElement) {
        this.svg = d3.select(containerElement).append("svg");
        this.force = d3.layout.force().charge(-1000).linkDistance(30).size([graphWidth,graphHeight*2]).gravity(0.5);
        this.graph = parseToGraph([]);

        this.force.nodes(this.graph.nodes).links(this.graph.links).start();

        this.renderGraph();

        var self = this;
        this.force.on("tick", function() {
            self.svg.selectAll(".link").attr("x1", function(d) { return d.source.x; })
                .attr("y1", function(d) { return d.source.y; })
                .attr("x2", function(d) { return d.target.x; })
                .attr("y2", function(d) { return d.target.y; });

            self.svg.selectAll(".node").attr("transform", function(d) { return "translate(" + d.x +","+ d.y +")"; });
          });
    }

    DependencyGraph.prototype.renderGraph = function() {
        var link = this.svg.selectAll(".link")
                      .data(this.graph.links)
                    .enter().append("line")
                      .attr("class", "link");

        var node = this.svg.selectAll(".node")
                      .data(this.graph.nodes)
                    .enter().append("g")
                      .attr("class", "node");

        node.append("circle")
            .attr("r", 7)
            .attr("class", "nodeCircle")
            .attr("fill", function(d) { return statusColors[d.status]; } );

        node.append("a").attr("xlink:href", function(d) { console.log(d); return d.trackingUrl; })
            .append("text").text(function(d) { return d.taskId; }).attr("y", -7);
    };

    DependencyGraph.prototype.updateData = function(taskList) {
        var newGraph = parseToGraph(taskList);
        this.graph.links = newGraph.links;
        this.graph.nodes = newGraph.nodes;

        this.force.nodes(this.graph.nodes).links(this.graph.links).start();
        
        this.renderGraph();
    };

    return {
        DependencyGraph: DependencyGraph
    };
})();
