function visualiserApp(luigi) {
    var templates = {};
    var invertDependencies = false;
    var typingTimer = 0;
    var visType = 'd3';
    var dt; // DataTable instantiated in $(document).ready()

    function loadTemplates() {
        $("script[type='text/template']").each(function(i, element) {
            var name = $(element).attr("name");
            var content = $(element).text();
            templates[name] = content;
        });
    }

    function renderTemplate(templateName, dataObject) {
        return $("<div>").html(Mustache.render(templates[templateName], dataObject));
    }


    function formatTime(dateObject) {
        return dateObject.getHours() + ":" + dateObject.getMinutes() + ":" + dateObject.getSeconds();
    }

    function taskToDisplayTask(showWorker, task) {
        var taskIdParts = /([A-Za-z0-9_]*)\((.*)\)/.exec(task.taskId);
        var taskName = taskIdParts[1];
        var taskParams = taskIdParts[2];
        var displayTime = new Date(Math.floor(task.start_time*1000)).toLocaleString();
        if (task.status == "RUNNING" && "time_running" in task) {
            var current_time = new Date().getTime();
            var minutes_running = Math.round((current_time - task.time_running * 1000) / 1000 / 60);
            displayTime += " | " + minutes_running + " minutes";
            if (showWorker && "worker_running" in task) {
              displayTime += " (" + task.worker_running + ")";
            }
        }
        return {
            taskId: task.taskId,
            taskName: taskName,
            taskParams: taskParams,
            priority: task.priority,
            resources: JSON.stringify(task.resources),
            displayTime: displayTime,
            displayTimestamp : task.start_time,
            trackingUrl: task.trackingUrl,
            status: task.status,
            graph: (task.status == "PENDING" || task.status == "RUNNING" || task.status == "DONE"),
            error: task.status == "FAILED",
            re_enable: task.status == "DISABLED" && task.re_enable_able
        };
    }

    function taskToRowData(task) {
        var taskIdParts = /([A-Za-z0-9_]*)\((.*)\)/.exec(task.taskId);
        var taskName = taskIdParts[1];
        var taskParams = taskIdParts[2];
        var displayTime = new Date(Math.floor(task.start_time*1000)).toLocaleString();
        //!TODO: time elapsed if running (see taskToDisplayTask)
        return {
            taskId: task.taskId,
            taskName: taskName,
            taskParams: taskParams,
            priority: task.priority,
            displayTime: displayTime,
            displayTimestamp : task.start_time,
            trackingUrl: task.trackingUrl,
            status: task.status,
            graph: (task.status == "PENDING" || task.status == "RUNNING" || task.status == "DONE"),
            error: task.status == "FAILED",
            re_enable: task.status == "DISABLED" && task.re_enable_able
        };
    }

    function taskCategoryIcon(category) {
        var iconClass;
        var iconColor;
        switch (category) {
            case 'PENDING':
                iconClass = 'fa-pause';
                iconColor = 'yellow';
                break;
            case 'RUNNING':
                iconClass = 'fa-play';
                iconColor = 'aqua';
                break;
            case 'DONE':
                iconClass = 'fa-check';
                iconColor = 'green';
                break;
            case 'FAILED':
                iconClass = 'fa-times';
                iconColor = 'red';
                break;
            case 'DISABLED':
                iconClass = 'fa-minus-circle';
                iconColor = 'gray';
                break;
            case 'UPSTREAM_FAILED':
                iconClass = 'fa-warning';
                iconColor = 'maroon';
                break;
            case 'UPSTREAM_DISABLED':
                iconClass = 'fa-warning';
                iconColor = 'gray';
                break;
            default:
                iconClass = 'fa-bug';
                iconColor = 'orange';
                break;
        }
        return '<span class="status-icon bg-' + iconColor + '"><i class="fa ' + iconClass + '"></i></span>';
    }


    /**
     * Filter table by all activated info boxes.
     */
    function filterByCategory(dt) {
        var infoBoxes = $('.info-box');

        var activeBoxes = [];
        infoBoxes.each(function (i) {
            if (infoBoxes[i].dataset.on === 'yes') {
                activeBoxes.push(infoBoxes[i].dataset.category);
            }
        });
        // Searched content will be <icon> <category>.
        var pattern = '\\b(' + activeBoxes.join('|') + ')\\b';
        dt.column(0).search(pattern, regex=true).draw();
    }

    function filterByTask(task, dt) {
        if (task === null) {
            dt.column(1).search('').draw();
        }
        else {
            dt.column(1).search('^' + task + '$', regex = true).draw();
        }
    }

    function toggleInfoBox(infoBox) {
        var infoBoxColor = infoBox.dataset.color;
        var infoBoxIcon = $(infoBox).find('.info-box-icon');
        var colorClass = 'bg-' + infoBoxColor;

        if ((infoBox.dataset.on === undefined) || (infoBox.dataset.on === 'no')) {
            infoBox.dataset.on = 'yes';
            infoBoxIcon.removeClass(colorClass);
            $(infoBox).addClass(colorClass);
        }
        else {
            infoBox.dataset.on = 'no';
            $(infoBox).removeClass(colorClass);
            infoBoxIcon.addClass(colorClass);
        }
    }

    function renderSidebar(tasks) {
        // tasks is a list of task names
        var counts = {};
        $.each(tasks, function(i) {
            var name = tasks[i];
            if (counts[name] === undefined) {
                counts[name] = 0;
            }
            counts[name] += 1;
        });
        var taskList = [];
        $.each(counts, function (name) {
            taskList.push({name: name, count: counts[name]});
        });
        return renderTemplate("sidebarTemplate", {"tasks": taskList});
    }

    function selectSidebarItem(item) {
        var sidebarItems = $('.sidebar').find('li');
        sidebarItems.each(function (i) {
            var item2 = sidebarItems[i];
            if (item2.dataset.task === undefined) {
                return;
            }
            if (item === item2) {
                if ($(item2).hasClass('active')) {
                    // item is active, deselect
                    $(item2).removeClass('active');
                    $(item2).find('.badge').removeClass('bg-green');
                }
                else {
                    // select item
                    $(item2).addClass('active');
                    $(item2).find('.badge').addClass('bg-green');
                }
            }
            else {
                // clear any selection
                $(item2).removeClass('active');
                $(item2).find('.badge').removeClass('bg-green');
            }
        });
    }

    function processWorker(worker) {
        worker.tasks = worker.running.map($.proxy(taskToDisplayTask, null, false));
        worker.start_time = new Date(worker.started * 1000).toLocaleString();
        worker.active = new Date(worker.last_active * 1000).toLocaleString();
        return worker;
    }

    function renderWorkers(workers) {
        return renderTemplate("workerTemplate", {"workers": workers.map(processWorker)});
    }

    function switchTab(tabId) {
        $(".tabButton").parent().removeClass("active");
        $(".tab-pane").removeClass("active");
        $("#"+tabId).addClass("active");
        $(".tabButton[data-tab="+tabId+"]").parent().addClass("active");
    }

    function showErrorTrace(error) {
        $("#errorModal").empty().append(renderTemplate("errorTemplate", error));
        $("#errorModal").modal({});
    }

    function makeGraphCallback(visType, taskId, paint) {
        function depGraphCallbackD3(dependencyGraph) {
            $("#searchError").empty();
            $("#searchError").removeClass();
            if(dependencyGraph.length > 0) {
                $("#dependencyTitle").text(taskId);
                if(dependencyGraph != '{}'){
                    for (var id in dependencyGraph) {
                        if (dependencyGraph[id].deps.length > 0) {
                            //console.log(asingInput(dependencyGraph, id));
                            dependencyGraph[id]['inputQueue']=asingInput(dependencyGraph, id);
                            dependencyGraph[id]['inputThroughput']=50;
                            dependencyGraph[id]['count']=5;
                            dependencyGraph[id]['consumers']=1;
                        }else{
                            dependencyGraph[id]['inputThroughput']=50;
                            dependencyGraph[id]['count']=5;
                            dependencyGraph[id]['consumers']=1;
                        }
                    }
                }
              bindGraphEvents();
            } else {
              $("#searchError").addClass("alert alert-error");
              $("#searchError").append("Couldn't find task " + taskId);
            }
            drawGraphETL(dependencyGraph, paint);
        }

        function depGraphCallback (dependencyGraph) {
            $("#graphPlaceholder svg").empty();
            $("#searchError").empty();
            $("#searchError").removeClass();
            if(dependencyGraph.length > 0) {
              $("#dependencyTitle").text(taskId);
              $("#graphPlaceholder").get(0).graph.updateData(dependencyGraph);
              $("#graphContainer").show();
              bindGraphEvents();
            } else {
              $("#searchError").addClass("alert alert-error");
              $("#searchError").append("Couldn't find task " + taskId);
            }
        }


        if (visType == 'd3') {
            return depGraphCallbackD3;
        }
        else {
            return depGraphCallback;
        }

    }

    function processHashChange(paint) {
        var hash = location.hash;
        if (hash == "#w") {
            switchTab("workerList");
        } else if (hash) {
            var taskId = hash.substr(1);
            $("#searchError").empty();
            $("#searchError").removeClass();
            if (taskId != "g") {
                var depGraphCallback = makeGraphCallback(visType, taskId, paint);

                if (invertDependencies) {
                    luigi.getInverseDependencyGraph(taskId, depGraphCallback);
                } else {
                    luigi.getDependencyGraph(taskId, depGraphCallback);
                    
                }
            }
            switchTab("dependencyGraph");
        } else {
            switchTab("taskList");
        }
    }

    function bindGraphEvents() {
        $(".graph-node-a").click(function(event) {
            var taskId = $(this).attr("data-task-id");
            var status = $(this).attr("data-task-status");
            if (status=="FAILED") {
                event.preventDefault();
                luigi.getErrorTrace(taskId, function(error) {
                   showErrorTrace(error);
                });
            }
        });
    }

    function bindListEvents() {
        $(window).on('hashchange', processHashChange);
        $("#invertCheckbox").click(function() {
            invertDependencies = this.checked;
            processHashChange(true);
        });
        $("a[href=#list]").click(function() { location.hash=""; });
        $("#loadTaskForm").submit(function(event) {
            event.preventDefault();
            location.hash = $(this).find("input").val();
        });

        $('.info-box').on('click', function () {
            toggleInfoBox(this);
            filterByCategory(dt);
        });

        $('#toggleVisButtons').on('click', 'label', function () {
            var newVisType = $(this).find('input')[0].value;
            initVisualisation(newVisType);
        });

    }



    function asingInput(worker, id){
        if (worker[id].deps.length > 0) {
            //console.log(worker[id].deps);
            return worker[id].deps;
        }
    }

    function getFinishTime(tasks, listId){
        var times = {};
        for (var i = 0; i < listId.length; i++) {
            for (var j = 0; j < tasks.length; j++) {
                if (listId[i]===tasks[j].taskId) {
                    var finishTime = new Date(tasks[j].time_running*1000);
                    var startTime = new Date(tasks[j].start_time*1000);
                    var durationTime = new Date((finishTime - startTime)*1000).getSeconds();
                    times[listId[i]] = durationTime;
                };
            };
        };
        return times;
    }
    function getParam(tasks, id){
        for (var i = 0; i < tasks.length; i++) {
            if (tasks[i].taskId === id) {
                return tasks[i].worker_running;
            };
        };
    }
    function getStatusTasks(tasks){
        var status;
        for (var i = 0; i < tasks.length; i++) {
            if (tasks[i].status === "DONE") {
                status = true;
            }else{
                return false;
            }
        };
        return status;
    }
    function drawGraphETL(tasks, paint){
    // Set up zoom support
        var svg = d3.select("#mysvg");
        var inner = svg.select("g"),
            zoom = d3.behavior.zoom().on("zoom", function() {
            inner.attr("transform", "translate(" + d3.event.translate + ")" +
                "scale(" + d3.event.scale + ")");
            });
        svg.call(zoom);

        // Create map of taskId to task
        var taskIdMap = {};
        $.each(tasks, function (i, task) {
            taskIdMap[task.taskId] = task;
        });

        var render = new dagreD3.render();
        // Left-to-right layout
        var g = new dagreD3.graphlib.Graph();
        g.setGraph({
            nodesep: 70,
            ranksep: 50,
            rankdir: "LR",
            marginx: 20,
            marginy: 20,
            height: 400,
            ranker: "longest-path"
        });

        function draw(isUpdate) {
            for (var id in tasks) {
                var task = tasks[id];
                var className = task.status;
                    
                var html = "<div onclick='window.location.href = \"" + "index.html#" + task.taskId + "\"'>";
                html += "<span class=status></span>";
                html += "<span class=name>"+task.name+"</span>";
                html += "<span class=queue><span class=counter>"+ task.status +"</span></span>";
                html += "</div>";
                g.setNode(task.taskId, {
                    labelType: "html",
                    label: html,
                    rx: 5,
                    ry: 5,
                    padding: 0,
                    class: className
                });
                if (task.inputQueue) {
                    for (var i =  0; i < task.inputQueue.length; i++) {
                        // Destination node may not be in tasks if this is an inverted graph
                        if (taskIdMap[task.inputQueue[i]] !== undefined) {
                            if (task.status === "DONE") {
                                var durationTime = getFinishTime(tasks, task.inputQueue);
                                g.setEdge(task.inputQueue[i], task.taskId, {
                                    label: durationTime[task.inputQueue[i]] + " secs",
                                    width: 40
                                });
                            } else {
                                g.setEdge(task.inputQueue[i], task.taskId, {
                                    width: 40
                                });
                            }
                        }
                    }

                }
            }
            var styleTooltip = function(name, description) {
                return "<p class='name'>" + name + "</p><p class='description'>" + description + "</p>";
            };
            inner.call(render, g);
            if(paint){
                // Zoom and scale to fit
                var zoomScale = zoom.scale();
                var graphWidth = g.graph().width + 80;
                var graphHeight = g.graph().height + 40;
                var width = parseInt(svg.style("width").replace(/px/, ""));
                var height = parseInt(svg.style("height").replace(/px/, ""));
                zoomScale = Math.min(width / graphWidth, height / graphHeight);
                var translate = [(width/2) - ((graphWidth*zoomScale)/2), (height/2) - ((graphHeight*zoomScale)/2)];
                zoom.translate(translate);
                zoom.scale(zoomScale);
                zoom.event(isUpdate ? svg.transition().duration(3000) : d3.select("#mysvg"));
            }

            inner.selectAll("g.node")
                .attr("title", function(v) { return styleTooltip(v, getParam(tasks, v)) })
                .each(function(v) { $(this).tipsy({ gravity: "w", opacity: 1, html: true }); });
        }
        draw();
    }

    /*
       DataTables functions
     */
    // Remove tasks of a given category and add new ones.
    function updateTaskCategory(dt, category, tasks) {
        var taskMap = {};

        var mostImportantCategory = function (cat1, cat2) {
            var priorities = [
                'RUNNING',
                'DONE',
                'PENDING',
                'UPSTREAM_DISABLED',
                'UPSTREAM_FAILED',
                'DISABLED',
                'FAILED'
            ];
            // NOTE : -1 indicates not in list
            var i1 = priorities.indexOf(cat1);
            var i2 = priorities.indexOf(cat2);
            var ret;
            if (i1 > i2) {
                ret = cat1;
            }
            else {
                ret = cat2;
            }
            return ret;
        };

        dt.rows(function (i, data) {
            taskMap[data.taskName] = data.category;
            return data.category === category;
        }).remove();

        var displayTasks = tasks.map(taskToRowData);
        displayTasks = displayTasks.filter(function (obj) {
            if (category === mostImportantCategory(category, taskMap[obj.taskName])) {
                obj.category = category;
                return true;
            }
            return false;
        });
        dt.rows.add(displayTasks);

        $('#'+category+'_info').find('.info-box-number').html(displayTasks.length);
        dt.draw();

        $('.sidebar').html(renderSidebar(dt.column(1).data()));
        $('.sidebar-menu').on('click', 'li', function () {
            if (this.dataset.task) {
                selectSidebarItem(this);
                if ($(this).hasClass('active')) {
                    filterByTask(this.dataset.task, dt);
                }
                else {
                    filterByTask(null, dt);
                }
            }
        });
    }

    function initVisualisation(newVisType) {
        visType = newVisType;

        // Prepare graphPlaceholder for D3 code
        if (visType == 'd3') {
            $('#graphPlaceholder').empty();
            $('#graphPlaceholder').html('<div class="live map"><svg width="100%" height="100%" id="mysvg"><g/></svg></div>');
        }
        else {
            $('#graphPlaceholder').empty();
            var graph = new Graph.DependencyGraph($("#graphPlaceholder")[0]);
            $("#graphPlaceholder")[0].graph = graph;
        }

        processHashChange(true);

    }

    $(document).ready(function() {
        loadTemplates();

        luigi.getWorkerList(function(workers) {
            $("#workerList").append(renderWorkers(workers));
        });

        dt = $('#taskTable').DataTable({
            columns: [
                {
                    data: 'category',
                    render: function (data, type, row) {
                        return taskCategoryIcon(data)+' '+data;
                    }
                },
                {data: 'taskName'},
                {data: 'taskParams'},
                {data: 'displayTime'},
                {
                    className: 'details-control',
                    orderable: false,
                    data: null,
                    render: function (data, type, row) {
                        return Mustache.render(templates['actionsTemplate'], row);
                    }
                }
            ]
        });
        
        luigi.getRunningTaskList(function(runningTasks) {
            updateTaskCategory(dt, 'RUNNING', runningTasks);
        });

        luigi.getFailedTaskList(function(failedTasks) {
            updateTaskCategory(dt, 'FAILED', failedTasks);
        });

        luigi.getUpstreamFailedTaskList(function(upstreamFailedTasks) {
            updateTaskCategory(dt, 'UPSTREAM_FAILED', upstreamFailedTasks);
        });

        luigi.getDisabledTaskList(function(disabledTasks) {
            updateTaskCategory(dt, 'DISABLED', disabledTasks);
        });

        luigi.getUpstreamDisabledTaskList(function(upstreamDisabledTasks) {
            updateTaskCategory(dt, 'UPSTREAM_DISABLED', upstreamDisabledTasks);
        });

        luigi.getPendingTaskList(function(pendingTasks) {
            updateTaskCategory(dt, 'PENDING', pendingTasks);
        });

        luigi.getDoneTaskList(function(doneTasks) {
            updateTaskCategory(dt, 'DONE', doneTasks);
        });

        bindListEvents();

        /* !TODO: Link errors
               luigi.getErrorTrace($(this).attr("data-task-id"), function(error) {
               showErrorTrace(error);
            });
         */

        //var graph = new Graph.DependencyGraph($("#graphPlaceholder")[0]);
        //$("#graphPlaceholder")[0].graph = graph;
        // Add event listener for opening and closing details

        $('#taskTable tbody').on('click', 'td.details-control .showError', function () {
            var tr = $(this).closest('tr');
            var row = dt.row( tr );
            var data = row.data();

            if ( row.child.isShown() ) {
                // This row is already open - close it
                row.child.hide();
            }
            else {
                // Open this row
                row.child(Mustache.render(templates['rowDetailsTemplate'], data)).show();

                // If error state is present retrieve the error
                if (data.error) {
                    errorTrace = row.child().find('.error-trace');
                    luigi.getErrorTrace(data.taskId, function(error) {
                        errorTrace.html('<pre class="pre-scrollable">'+error.error+'</pre>');
                    });
                }


            }

        } );

        initVisualisation(visType);

        debugHook = initVisualisation;
    });
}
