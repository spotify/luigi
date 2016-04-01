function visualiserApp(luigi) {
    var templates = {};
    var invertDependencies = false;
    var hideDone = false;
    var typingTimer = 0;
    var visType;
    var dt; // DataTable instantiated in $(document).ready()
    var missingCategories = {};
    var currentFilter = {
        taskFamily: "",
        taskCategory: [],
        tableFilter: ""
    };
    var taskIcons = {
        PENDING: 'pause',
        RUNNING: 'play',
        DONE: 'check',
        FAILED: 'times',
        UPSTREAM_FAILED: 'warning',
        DISABLED: 'minus-circle',
        UPSTREAM_DISABLED: 'warning'
    }

    function getVisType() {
        var cookieParts = document.cookie.match(/visType=(.*)/);
        if (cookieParts === null) {
            return 'svg';
        }
        else {
            return cookieParts[1];
        }
    }
    function setVisType (newVisType) {
        visType = newVisType;
        document.cookie = 'visType=' + visType;

        $('#toggleVisButtons').find('label').removeClass('active');
        $('#toggleVisButtons').find('input[value="' + visType + '"]').parent().addClass('active');

        return visType;
    }


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

    function taskToDisplayTask(task) {
        var taskName = task.name;
        var taskParams = JSON.stringify(task.params);
        var displayTime = new Date(Math.floor(task.last_updated*1000)).toLocaleString();
        var time_running = -1;
        if (task.status == "RUNNING" && "time_running" in task) {
            var current_time = new Date().getTime();
            var minutes_running = Math.round((current_time - task.time_running * 1000) / 1000 / 60);
            time_running = task.time_running;
            displayTime += " | " + minutes_running + " minutes";
        }
        return {
            taskId: task.taskId,
            encodedTaskId: encodeURIComponent(task.taskId),
            taskName: taskName,
            taskParams: taskParams,
            displayName: task.display_name,
            priority: task.priority,
            resources: JSON.stringify(task.resources),
            displayTime: displayTime,
            displayTimestamp: task.last_updated,
            timeRunning: time_running,
            trackingUrl: task.tracking_url,
            status: task.status,
            graph: (task.status == "PENDING" || task.status == "RUNNING" || task.status == "DONE"),
            error: task.status == "FAILED",
            re_enable: task.status == "DISABLED" && task.re_enable_able,
            statusMessage: task.status_message
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
        currentFilter.taskCategory = activeBoxes;
        dt.column(0).search(pattern, regex=true).draw();
    }

    function filterByTaskFamily(taskFamily, dt) {
        currentFilter.taskFamily = taskFamily;
        if (taskFamily === "") {
            dt.column(1).search('').draw();
        }
        else {
            dt.column(1).search('^' + taskFamily + '$', regex = true).draw();
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
        taskList.sort(function(a,b){
          return a.name.localeCompare(b.name);
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

    function renderWarnings() {
        return renderTemplate("warningsTemplate",
            {missingCategories: $.map(missingCategories, function (v, k) {return v})}
        );
    }

    function processWorker(worker) {
        worker.encoded_first_task = encodeURIComponent(worker.first_task);
        worker.tasks = worker.running.map(taskToDisplayTask);
        worker.tasks.sort(function(task1, task2) { return task1.timeRunning - task2.timeRunning; });
        worker.start_time = new Date(worker.started * 1000).toLocaleString();
        worker.active = new Date(worker.last_active * 1000).toLocaleString();
        return worker;
    }

    function renderWorkers(workers) {
        return renderTemplate("workerTemplate", {"workers": workers.map(processWorker)});
    }

    function processResource(resource) {
        resource.tasks = resource.running.map(taskToDisplayTask);
        resource.percent_used = 100 * resource.num_used / resource.num_total;
        if (resource.percent_used >= 100) {
            resource.bar_type = 'danger';
        } else if (resource.percent_used > 50) {
            resource.bar_type = 'warning';
        } else {
            resource.bar_type = 'success';
        }
        return resource;
    }

    function renderResources(resources) {
        return renderTemplate("resourceTemplate", {
            "resources": resources.map(processResource).sort(function(r1, r2) {
                if (r1.percent_used > r2.percent_used)
                    return -1;
                else if (r1.percent_used < r2.percent_used)
                    return 1;
                else if (r1.num_used > r2.num_used)
                    return -1;
                else if (r1.num_used < r2.num_used)
                    return 1;
                else if (r1.name < r2.name)
                    return -1;
                else if (r1.name > r2.name)
                    return 1;
                else
                    return 0;
            })
        });
    }

    function switchTab(tabId) {
        $(".tabButton").parent().removeClass("active");
        $(".tab-pane").removeClass("active");
        $("#"+tabId).addClass("active");
        $(".tabButton[data-tab="+tabId+"]").parent().addClass("active");
        updateSidebar(tabId);
    }

    function showErrorTrace(data) {
        data.error = decodeError(data.error)
        $("#errorModal").empty().append(renderTemplate("errorTemplate", data));
        $("#errorModal").modal({});
    }

    function showStatusMessage(data) {
        $("#statusMessageModal").empty().append(renderTemplate("statusMessageTemplate", data));
        $("#statusMessageModal .refresh").on('click', function() {
            luigi.getTaskStatusMessage(data.taskId, function(data) {
                $("#statusMessageModal pre").html(data.statusMessage);
            });
        }).trigger('click');
        $("#statusMessageModal").modal({});
    }

    function preProcessGraph(dependencyGraph) {
        var extraNodes = [];
        var seen = {};
        $.each(dependencyGraph, function(i, node) {
            seen[node.taskId] = true;
        });
        $.each(dependencyGraph, function(i, node) {
            $.each(node.deps, function(j, dep) {
                if (!seen[dep]) {
                    seen[dep] = true;
                    var paramsStrs = (/\((.*)\)/.exec(dep) || ['', ''])[1].split(', ');
                    var params = {};
                    $.each(paramsStrs, function(i, param) {
                        if (param !== "") {
                            var kv = param.split('=');
                            params[kv[0]] = kv[1];
                        }
                    });

                    extraNodes.push({
                        name: (/(\w+)\(/.exec(dep) || [])[1],
                        taskId: dep,
                        deps: [],
                        params: params,
                        status: "TRUNCATED"
                    });
                }
            });
        });
        return dependencyGraph.concat(extraNodes);
    }

    function makeGraphCallback(visType, taskId, paint) {
        function depGraphCallbackD3(dependencyGraph) {
            $("#searchError").empty();
            $("#searchError").removeClass();
            if(dependencyGraph.length > 0) {
                $("#dependencyTitle").text(dependencyGraph[0].display_name);
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
            } else {
              $("#searchError").addClass("alert alert-error");
              $("#searchError").append("Couldn't find task " + taskId);
            }
            drawGraphETL(dependencyGraph, paint);
            bindGraphEvents();
        }

        function depGraphCallback (dependencyGraph) {
            $("#graphPlaceholder svg").empty();
            $("#searchError").empty();
            $("#searchError").removeClass();
            if(dependencyGraph.length > 0) {
              $("#dependencyTitle").text(dependencyGraph[0].display_name);
              $("#graphPlaceholder").get(0).graph.updateData(dependencyGraph);
              $("#graphContainer").show();
              bindGraphEvents();
            } else {
              $("#searchError").addClass("alert alert-error");
              $("#searchError").append("Couldn't find task " + taskId);
            }
        }

        function processedCallback(callback) {
            function processed(dependencyGraph) {
                return callback(preProcessGraph(dependencyGraph));
            }
            return processed;
        }


        if (visType == 'd3') {
            return processedCallback(depGraphCallbackD3);
        }
        else {
            return processedCallback(depGraphCallback);
        }

    }

    function processHashChange(paint) {
        var hash = decodeURIComponent(location.hash);
        if (hash == "#w") {
            switchTab("workerList");
        } else if (hash == "#r") {
            switchTab("resourceList");
        } else if (hash) {
            var taskId = decodeURIComponent(hash.substr(1));
            $("#searchError").empty();
            $("#searchError").removeClass();
            if (taskId != "g") {
                var depGraphCallback = makeGraphCallback(visType, taskId, paint);

                if (invertDependencies) {
                    luigi.getInverseDependencyGraph(taskId, depGraphCallback, !hideDone);
                } else {
                    luigi.getDependencyGraph(taskId, depGraphCallback, !hideDone);
                }
            }
            switchTab("dependencyGraph");
        } else {
            switchTab("taskList");
        }
    }

    function bindGraphEvents() {
        if (visType === 'd3') {
            $('.node').click(function(event) {
                var taskDiv = $(this).find('.taskNode');
                var taskId = taskDiv.attr("data-task-id");
                event.preventDefault();
                // NOTE : hasClass() not reliable inside SVG
                if ($(this).attr('class').match(/\bFAILED\b/)) {
                    luigi.getErrorTrace(taskId, function (error) {
                        showErrorTrace(error);
                    });
                }
                else {
                    window.location.href = 'index.html#' + taskId;
                }
            })
        }
        else {
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
    }

    function bindListEvents() {
        $(window).on('hashchange', processHashChange);
        invertDependencies = $('#invertCheckbox')[0].checked;
        $("#invertCheckbox").click(function() {
            invertDependencies = this.checked;
            processHashChange(true);
        });
        hideDone = $('#hideDoneCheckbox')[0].checked;
        $('#hideDoneCheckbox').click(function() {
            hideDone = this.checked;
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
            setVisType($(this).find('input')[0].value);
            initVisualisation(visType);
        });

        /*
          Note: The #filter-input element is used by LuigiAPI to constrain requests to the server.
          When the accompanying button is pressed we force a reload.
         */
        $('#serverSide').on('change', 'label', function () {
            updateTasks();
        })

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

                var html = "<div class='taskNode' data-task-id='" + task.taskId + "'>";
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
            taskMap[data.taskId] = data.category;
            return data.category === category;
        }).remove();

        var taskCount;
        /* Check for integers in tasks.  This indicates max-shown-tasks was exceeded */
        if (tasks.length === 1 && typeof(tasks[0]) === 'number') {
            taskCount = tasks[0];
            missingCategories[category] = {name: category, count: taskCount};
        }
        else {
            var displayTasks = tasks.map(taskToDisplayTask);
            displayTasks = displayTasks.filter(function (obj) {
                if (obj === null) {
                    return false;
                }
                if (category === mostImportantCategory(category, taskMap[obj.taskId])) {
                    obj.category = category;
                    return true;
                }
                return false;
            });
            dt.rows.add(displayTasks);
            taskCount = displayTasks.length;
            delete missingCategories[category];
        }


        $('#'+category+'_info').find('.info-box-number').html(taskCount);
        $('#'+category+'_info i.fa').removeClass().addClass('fa fa-'+taskIcons[category]);

    }

    function updateCurrentFilter() {
        var content;
        currentFilter.tableFilter = dt.search();

        if ((currentFilter.tableFilter === "") &&
            ($.isEmptyObject(currentFilter.taskCategory)) &&
            (currentFilter.taskFamily === "")) {

            content = '';
        }
        else {
            if (currentFilter.taskCategory !== "") {
                currentFilter.catNames = $.map(currentFilter.taskCategory, function (x) {
                    return {name: x};
                });
            }

            content = renderTemplate('currentFilterTemplate', currentFilter);
        }

        $('#currentFilter').html(content);
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

    function updateTasks() {
        $('.status-info .info-box-number').text('?');
        $('.status-info i.fa').removeClass().addClass('fa fa-spinner fa-pulse');

        var ajax1 = luigi.getRunningTaskList(function(runningTasks) {
            updateTaskCategory(dt, 'RUNNING', runningTasks);
        });

        var ajax2 = luigi.getFailedTaskList(function(failedTasks) {
            updateTaskCategory(dt, 'FAILED', failedTasks);
        });

        var ajax3 = luigi.getUpstreamFailedTaskList(function(upstreamFailedTasks) {
            updateTaskCategory(dt, 'UPSTREAM_FAILED', upstreamFailedTasks);
        });

        var ajax4 = luigi.getDisabledTaskList(function(disabledTasks) {
            updateTaskCategory(dt, 'DISABLED', disabledTasks);
        });

        var ajax5 = luigi.getUpstreamDisabledTaskList(function(upstreamDisabledTasks) {
            updateTaskCategory(dt, 'UPSTREAM_DISABLED', upstreamDisabledTasks);
        });

        var ajax6 = luigi.getPendingTaskList(function(pendingTasks) {
            updateTaskCategory(dt, 'PENDING', pendingTasks);
        });

        var ajax7 = luigi.getDoneTaskList(function(doneTasks) {
            updateTaskCategory(dt, 'DONE', doneTasks);
        });

        $.when(ajax1, ajax2, ajax3, ajax4, ajax5, ajax6, ajax7).done(function () {
            dt.draw();

            $('.sidebar').html(renderSidebar(dt.column(1).data()));
            var selectedFamily = $('.sidebar-menu').find('li[data-task="' + currentFilter.taskFamily + '"]')[0];
            selectSidebarItem(selectedFamily);
            $('.sidebar-menu').on('click', 'li', function () {
                if (this.dataset.task) {
                    selectSidebarItem(this);
                    if ($(this).hasClass('active')) {
                        filterByTaskFamily(this.dataset.task, dt);
                    }
                    else {
                        filterByTaskFamily("", dt);
                    }
                }
            });

            if ($.isEmptyObject(missingCategories)) {
                $('#warnings').html('');
            }
            else {
                $('#warnings').html(renderWarnings());
            }
        });

    }

    function updateSidebar(tabName) {
        if (tabName === 'taskList') {
            $('body').removeClass('sidebar-collapse');
        }
        else {
            $('body').addClass('sidebar-collapse');
        }

    }

    // Error strings may or may not be JSON encoded, depending on client version
    // Decoding an unencoded string may raise an exception.
    function decodeError(error) {
        var decoded;
        try {
            decoded = JSON.parse(error);
        }
        catch (e) {
            decoded = error;
        }
        return decoded;
    }

    /**
     * Return HTML of a task parameter dictionary
     * @param params: task parameter dictionary
     */
    function renderParams(params) {
        var htmls = [];
        for (var key in params) {
            htmls.push('<span class="param-name">' + key +
                '</span>=<span class="param-value">' + params[key] + '</span>');
        }
        return htmls.join(', ');
    }

    $(document).ready(function() {
        loadTemplates();

        luigi.getWorkerList(function(workers) {
            $("#workerList").append(renderWorkers(workers));

            $('.worker-table tbody').on('click', 'td .statusMessage', function() {
                var data = $(this).data();
                showStatusMessage(data);
            });
        });

        luigi.getResourceList(function(resources) {
            $("#resourceList").append(renderResources(resources));
        });

        dt = $('#taskTable').DataTable({
            dom: 'l<"#serverSide">frtip',
            language: {
                search: 'Filter table:'
            },
            columns: [
                {
                    data: 'category',
                    render: function (data, type, row) {
                        return taskCategoryIcon(data)+' '+data;
                    }
                },
                {data: 'taskName'},
                {
                    data: 'taskParams',
                    render: function(data, type, row) {
                        var params = JSON.parse(data);
                        if (row.resources !== '{}') {
                            return '<div>' + renderParams(params) + '</div><div>' + row.resources + '</div>';
                        }
                        else {
                            return '<div>' + renderParams(params) + '</div>';
                        }
                    }
                },
                {data: 'priority', width: "2em"},
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

        dt.on('draw', updateCurrentFilter);

        $('#serverSide').html('<form class="form-inline"><label class="btn btn-default" for="serverSideCheckbox">' +
                      'Filter on Server <input type="checkbox" id="serverSideCheckbox"/>' +
                      '</label></form>');

        // If using server-side filter we need to updateTasks every time the filter changes

        $('#taskTable_filter').on('keyup paste', 'input', function () {
            if ($('#serverSideCheckbox')[0].checked) {
                clearTimeout(typingTimer);
                if ($(this).val) {
                    typingTimer = setTimeout(updateTasks, 400);
                }
            }
        });


        updateTasks();
        bindListEvents();

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
                        errorTrace.html('<pre class="pre-scrollable">'+decodeError(error.error)+'</pre>');
                    });
                }


            }

        } );

        $('#taskTable tbody').on('click', 'td.details-control .re-enable-button', function () {
            var that = $(this);
            luigi.reEnable($(this).attr("data-task-id"), function(data) {
                updateTasks();
            });
        });

        $('#taskTable tbody').on('click', 'td.details-control .statusMessage', function () {
            var data = $(this).data();
            showStatusMessage(data);
        });

        $('.navbar-nav').on('click', 'a', function () {
            var tabName = $(this).data('tab');
            updateSidebar(tabName);
        });

        $('#workerList').on('show.bs.modal', '#disableWorkerModal', function (event) {
            var triggerButton = $(event.relatedTarget);
            $('#disableWorkerButton').data('trigger', triggerButton);
        });

        $('#workerList').on('click', '#disableWorkerButton', function() {
            var triggerButton = $(this).data('trigger');
            var worker = triggerButton.data('worker');

            luigi.disableWorker(worker);

            // show the worker as disabled in the visualiser
            triggerButton.parents('.box').addClass('box-solid box-default');
            triggerButton.remove();
        });

        visType = getVisType();
        setVisType(visType);
        initVisualisation(visType);

    });
}
