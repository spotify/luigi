Graph = (function() {
    var statusColors = {
        "FAILED":"#DD0000",
        "RUNNING":"#0044DD",
        "BATCH_RUNNING":"#BB00BB",
        "PENDING":"#EEBB00",
        "DONE":"#00DD00",
        "DISABLED":"#808080",
        "UNKNOWN":"#000000",
        "TRUNCATED":"#FF00FF"
    };

    /* Line height for items in task status legend */
    var legendLineHeight = 20;

    /* Height of vertical space between nodes */
    var nodeHeight = 10;

    /* Amount of horizontal space given for each node */
    var nodeWidth = 200;

    /* Calculate minimum SVG height required for legend */
    var legendMaxY = (function () {
        return Object.keys(statusColors).length * legendLineHeight + ( legendLineHeight / 2 )
    })();

    var legendWidth = 110;

    function nodeFromTask(task) {
        var deps = task.deps;
        deps.sort();
        return {
            name: task.name,
            taskId: task.taskId,
            status: task.status,
            trackingUrl: this.hashBase + task.taskId,
            deps: deps,
            params: task.params,
            priority: task.priority,
            depth: -1
        };
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
                if (nodeIndex[dep]) {
                    edges.push({
                        source: nodes[nodeIndex[task.taskId]],
                        target: nodes[nodeIndex[dep]]
                    });
                }
            });
        });
        return edges;
    }

    /* Compute the maximum depth of each node for layout purposes, returns the number
       of nodes at each depth level (for layout purposes) */
    function computeDepth(nodes, nodeIndex) {
        function descend(n, depth) {
            if (n.depth === undefined || depth > n.depth) {
                n.depth = depth;
                $.each(n.deps, function(i, dep) {
                    if (nodeIndex[dep]) {
                        descend(nodes[nodeIndex[dep]], depth + 1);
                    }
                });
            }
        }
        descend(nodes[0], 0);

        var rowSizes = [];
        function placeNodes(n, depth) {
            if (rowSizes[depth] === undefined) {
                rowSizes[depth] = 0;
            }
            if (n.xOrder === undefined && depth === n.depth) {
                n.xOrder = rowSizes[depth];
                rowSizes[depth]++;
                $.each(n.deps, function(i, dep) {
                    if (nodeIndex[dep]) {
                        placeNodes(nodes[nodeIndex[dep]], depth + 1);
                    }
                });
            }
        }
        placeNodes(nodes[0], 0);

        return rowSizes;
    }

    /* Format nodes according to their depth and horizontal sort order.
       Algorithm: evenly distribute nodes along each depth level, offsetting each
       by the text line height to prevent overlapping text. This is done within
       multiple columns to keep the levels from being too tall. The column width
       is at least nodeWidth to ensure readability. The height of each level is
       determined by number of nodes divided by number of columns, rounded up. */
    function layoutNodes(nodes, rowSizes) {
        var numCols = Math.max(2, Math.floor(graphWidth / nodeWidth));
        function rowStartPosition(depth) {
            if (depth === 0) return 20;
            var rowHeight = Math.ceil(rowSizes[depth-1] / numCols);
            return rowStartPosition(depth-1)+Math.max(rowHeight * nodeHeight + 100);
        }
        $.each(nodes, function(i, node) {
            var numRows = Math.ceil(rowSizes[node.depth] / numCols);
            var levelCols = Math.ceil(rowSizes[node.depth] / numRows);
            var row = node.xOrder % numRows;
            var col = node.xOrder / numRows;
            node.x = ((col + 1) / (levelCols + 1)) * (graphWidth - 200);
            node.y = rowStartPosition(node.depth) + row * nodeHeight;
        });
    }

    /* Parses a list of tasks to a graph format */
    function createGraph(tasks, hashBase) {
        if (tasks.length === 0) return {nodes: [], links: []};

        this.hashBase = hashBase;
        var nodes = $.map(tasks, nodeFromTask);
        var nodeIndex = uniqueIndexByProperty(nodes, "taskId");

        var rowSizes = computeDepth(nodes, nodeIndex);

        nodes = $.map(nodes, function(node) { return node.depth >= 0 ? node: null; });

        layoutNodes(nodes, rowSizes);

        // We need to re-index nodes after filtering
        nodeIndex = uniqueIndexByProperty(nodes, "taskId");
        var edges = createDependencyEdges(nodes, nodeIndex);

        return {
            nodes: nodes,
            links: edges
        };
    }

    function findBounds(nodes) {
        var maxX = 0;
        var maxY = legendMaxY;
        $.each(nodes, function(i, node) {
            if (node.x>maxX) maxX = node.x;
            if (node.y>maxY) maxY = node.y;
        });
        return {
            x:maxX,
            y:maxY
        };
    }

    var graphWidth = window.innerWidth - 80;

    function DependencyGraph(containerElement) {
        this.svg = $(svgElement("svg")).appendTo($(containerElement));
    }


    /* We need custom element creators for svg nodes and xlink attributes because jQuery doesn't support
       namespaces properly */
    function svgElement(name) {
        return document.createElementNS("http://www.w3.org/2000/svg", name);
    }

    function svgLink(url) {
        var element = svgElement("a");
        element.setAttributeNS("http://www.w3.org/1999/xlink", "href", url);
        return element;
    }

    DependencyGraph.prototype.renderGraph = function() {
        var self = this;

        $.each(this.graph.links, function(i, link) {
            var line = $(svgElement("line"))
                        .attr("class","link")
                        .attr("x1", link.source.x)
                        .attr("y1", link.source.y)
                        .attr("x2", link.target.x)
                        .attr("y2", link.target.y)
                        .appendTo(self.svg);
        });

        $.each(this.graph.nodes, function(i, node) {
            var g = $(svgElement("g"))
                .addClass("node")
                .attr("transform", "translate(" + node.x + "," + node.y +")")
                .appendTo(self.svg);

            $(svgElement("circle"))
                .addClass("nodeCircle")
                .attr("r", 7)
                .attr("fill", statusColors[node.status])
                .appendTo(g);
            $(svgLink(node.trackingUrl))
                .append(
                    $(svgElement("text"))
                    .text(node.name)
                    .attr("y", 3))
                .attr("class","graph-node-a")
                .attr("data-task-status", node.status)
                .attr("data-task-id", node.taskId)
                .appendTo(g);

            var titleText = node.name;
            var content = $.map(node.params, function (value, name) { return name + ": " + value; }).join("<br>");
            g.attr("title", titleText)
                .popover({
                    trigger: 'hover',
                    container: 'body',
                    html: true,
                    placement: 'top',
                    content: content
                });
        });

        // Legend for Task status
        var legend = $(svgElement("g"))
                .addClass("legend")
                .appendTo(self.svg);

        $(svgElement("rect"))
            .attr("x", -1)
            .attr("y", -1)
            .attr("width", legendWidth + "px")
            .attr("height", legendMaxY + "px")
            .attr("fill", "#FFF")
            .attr("stroke", "#DDD")
            .appendTo(legend);

        var x = 0;
        $.each(statusColors, function(key, color) {
            var c = $(svgElement("circle"))
                .addClass("nodeCircle")
                .attr("r", 7)
                .attr("cx", legendLineHeight)
                .attr("cy", (legendLineHeight-4)+(x*legendLineHeight))
                .attr("fill", color)
                .appendTo(legend);

            $(svgElement("text"))
                .text(key.charAt(0).toUpperCase() + key.substring(1).toLowerCase().replace(/_./gi, function (x) { return " " + x[1].toUpperCase(); }))
                .attr("x", legendLineHeight + 14)
                .attr("y", legendLineHeight+(x*legendLineHeight))
                .appendTo(legend);

            x++;
        });
    };

    DependencyGraph.prototype.updateData = function(taskList, hashBase) {
        $('.popover').popover('destroy');
        this.graph = createGraph(taskList, hashBase);
        bounds = findBounds(this.graph.nodes);
        this.renderGraph();
        this.svg.attr("height", bounds.y+10);
        this.svg.attr("width", graphWidth+10);
        this.svg[0].setAttributeNS("http://www.w3.org/2000/svg", "preserveAspectRatio", "xMidYMid meet");
        this.svg[0].setAttributeNS("http://www.w3.org/2000/svg", "viewBox", "0 0 " + graphWidth + " " + (bounds.y+10));
    };

    return {
        DependencyGraph: DependencyGraph,
        testableMethods: {
            nodeFromTask: nodeFromTask,
            uniqueIndexByProperty: uniqueIndexByProperty,
            createDependencyEdges: createDependencyEdges,
            computeDepth: computeDepth,
            createGraph: createGraph,
            findBounds: findBounds
        }
    };
})();
