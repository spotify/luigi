function visualiserApp(luigi) {
    var templates = {};
    var typingTimer = 0;
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
        BATCH_RUNNING: 'play',
        DONE: 'check',
        FAILED: 'times',
        UPSTREAM_FAILED: 'warning',
        DISABLED: 'minus-circle',
        UPSTREAM_DISABLED: 'warning'
    };
    var VISTYPE_DEFAULT = 'svg';

    /*
     * Updates view of the Visualization type.
     */
    function updateVisType(newVisType) {
        $('#toggleVisButtons label').removeClass('active');
        var visTypeInput = $('#toggleVisButtons input[value="' + newVisType + '"]');
        visTypeInput.parent().addClass('active');
        visTypeInput.prop('checked', true);
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
            resources: JSON.stringify(task.resources_running || task.resources).replace(/,"/g, ', "'),
            displayTime: displayTime,
            displayTimestamp: task.last_updated,
            timeRunning: time_running,
            trackingUrl: task.tracking_url,
            status: task.status,
            graph: (task.status == "PENDING" || task.status == "RUNNING" || task.status == "DONE"),
            error: task.status == "FAILED",
            re_enable: task.status == "DISABLED" && task.re_enable_able,
            mark_as_done: (task.status == "RUNNING" || task.status == "FAILED" || task.status == "DISABLED"),
            statusMessage: task.status_message,
            progressPercentage: task.progress_percentage,
            acceptsMessages: task.accepts_messages,
            workerIdRunning: task.worker_running,
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
            case 'BATCH_RUNNING':
                iconClass = 'fa-play';
                iconColor = 'purple';
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
    function filterByCategory(dt, activeBoxes) {
        if (activeBoxes === undefined) {
            activeBoxes = getActiveBoxes();
        }
        currentFilter.taskCategory = activeBoxes;
        dt.column(0).search(categoryQuery(activeBoxes), regex=true).draw();
    }

    function categoryQuery(activeBoxes) {
        // Searched content will be <icon> <category>.
        return '\\b(' + activeBoxes.join('|') + ')\\b';
    }

    function getActiveBoxes() {
        var infoBoxes = $('.info-box');

        var activeBoxes = [];
        infoBoxes.each(function (i) {
            if (infoBoxes[i].dataset.on === 'yes') {
                activeBoxes.push(infoBoxes[i].dataset.category);
            }
        });
        return activeBoxes;
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

    function toggleInfoBox(infoBox, activate) {
        var infoBoxColor = infoBox.dataset.color;
        var infoBoxIcon = $(infoBox).find('.info-box-icon');
        var colorClass = 'bg-' + infoBoxColor;

        if ((infoBox.dataset.on === undefined) || (infoBox.dataset.on === 'no') || activate) {
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
            var dotIndex = name.indexOf('.');
            var prefix = 'Others';
            if (dotIndex > 0) {
                prefix = name.slice(0, dotIndex);
            }
            var prefixList = taskList.find(function (pref) {
                return pref.name == prefix;
            })
            if (prefixList) {
                prefixList.tasks.push({name: name, count: counts[name]});
            } else {
                prefixList = {
                    name: prefix,
                    tasks: [{name: name, count: counts[name]}]
                }
                taskList.push(prefixList);
            }

        });
        taskList.sort(function(a,b){
            if (a.name == 'Others') {
                if (b.name == 'Others') {
                    return 0;
                }
                return 1;
            } else if (b.name == 'Others') {
                return -1;
            }
            return a.name.localeCompare(b.name);
        });
        taskList.forEach(function(p){
            p.tasks.sort(function(a,b){
                return a.name.localeCompare(b.name);
            });
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
        return renderTemplate(
            "warningsTemplate",
            {missingCategories: $.map(missingCategories, function (v, k) {return v;})}
        );
    }

    function processWorker(worker) {
        worker.encoded_first_task = encodeURIComponent(worker.first_task);
        worker.tasks = worker.running.map(taskToDisplayTask);
        worker.tasks.sort(function(task1, task2) { return task1.timeRunning - task2.timeRunning; });
        worker.start_time = new Date(worker.started * 1000).toLocaleString();
        worker.active = new Date(worker.last_active * 1000).toLocaleString();
        worker.is_disabled = worker.state === 'disabled';
        return worker;
    }

    function renderWorkers(workers) {
        return renderTemplate("workerTemplate", {"workerList": workers.map(processWorker)});
    }

    function processResource(resource) {
        resource.tasks = resource.running.map(taskToDisplayTask);
        resource.percent_used = 100 * resource.num_used / resource.num_total;
        if (resource.percent_used >= 100) {
            resource.bar_type = 'danger';
            resource.percent_used = 100;
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
        $("#" + tabId).addClass("active");
        $(".navbar-nav li").removeClass("active");
        $(".js-nav-link[data-tab=" + tabId + "]").parent().addClass("active");
        updateSidebar(tabId);
    }

    function showErrorTrace(data) {
        data.error = decodeError(data.error);
        $("#errorModal").empty().append(renderTemplate("errorTemplate", data));
        $("#errorModal").modal({});
    }

    function showStatusMessage(data) {
        $("#statusMessageModal").empty().append(renderTemplate("statusMessageTemplate", data));
        $("#statusMessageModal").modal({});
        var refreshInterval = setInterval(function() {
                if ($("#statusMessageModal").is(":hidden"))
                    clearInterval(refreshInterval);
                else {
                    luigi.getTaskStatusMessage(data.taskId, function(data) {
                        if (data.statusMessage === null)
                            $("#statusMessageModal pre").hide();
                        else {
                            $("#statusMessageModal pre").html(data.statusMessage).show();
                        }
                    });
                    luigi.getTaskProgressPercentage(data.taskId, function(data) {
                        // show or hide the progress bar container in the message modal
                        $("#statusMessageModal .progress").toggle(data.progressPercentage !== null);

                        // adjust the status of both progress bars (message modal and worker list)
                        var value = data.progressPercentage || 0;
                        var progressBars = $('#statusMessageModal .progress-bar, ' +
                            '.worker-table tbody .taskProgressBar[data-task-id="' + data.taskId + '"]');
                        progressBars.attr('aria-valuenow', value)
                            .text(value + '%')
                            .css({'width': value + '%'});
                    });
                }
            },
            500
        );
    }

    function showSchedulerMessageModal(data) {
        var $modal = $("#schedulerMessageModal");

        $modal.empty().append(renderTemplate("schedulerMessageTemplate", data));
        var $input = $modal.find("#schedulerMessageInput");
        var $send = $modal.find("#schedulerMessageButton");
        var $awaitResponse = $modal.find("#schedulerMessageAwaitResponse");
        var $responseContainer = $modal.find("#schedulerMessageResponse");
        var $responseSpinner = $responseContainer.find("pre > i");
        var $responseContent = $responseContainer.find("pre > div");

        $input.on("keypress", function($event) {
            if (event.keyCode == 13) {
                $send.trigger("click");
                $event.preventDefault();
            }
        });

        $send.on("click", function($event) {
            var content = $input.val();
            var awaitResponse = $awaitResponse.prop("checked");
            if (content && data.worker) {
                if (awaitResponse) {
                    $responseContainer.show();
                    $responseSpinner.show();
                    $responseContent.empty();
                    luigi.sendSchedulerMessage(data.worker, data.taskId, content, function(messageId) {
                        var interval = window.setInterval(function() {
                            luigi.getSchedulerMessageResponse(data.taskId, messageId, function(response) {
                                if (response != null) {
                                    clearInterval(interval);
                                    $responseSpinner.hide();
                                    $responseContent.html(response);
                                }
                            });
                        }, 1000);
                    });
                    $event.stopPropagation();
                } else {
                    $responseContainer.hide();
                    luigi.sendSchedulerMessage(data.worker, data.taskId, content);
                }
            }
        });

        $modal.on("shown.bs.modal", function() {
            $input.focus();
        });

        $modal.modal({});
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
                            dependencyGraph[id].inputQueue = asingInput(dependencyGraph, id);
                            dependencyGraph[id].inputThroughput = 50;
                            dependencyGraph[id].count = 5;
                            dependencyGraph[id].consumers = 1;
                        }else{
                            dependencyGraph[id].inputThroughput = 50;
                            dependencyGraph[id].count = 5;
                            dependencyGraph[id].consumers = 1;
                        }
                    }
                }
            } else {
                $("#searchError").addClass("alert alert-error");
                $("#searchError").text("Couldn't find task " + taskId);
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
                var hashBaseObj = URI.parseQuery(location.hash.replace('#', ''));
                delete hashBaseObj.taskId;
                var hashBase = '#' + URI.buildQuery(hashBaseObj) + '&taskId=';
                $("#graphPlaceholder").get(0).graph.updateData(dependencyGraph, hashBase);
                $("#graphContainer").show();
                bindGraphEvents();
            } else {
                $("#searchError").addClass("alert alert-error");
                $("#searchError").text("Couldn't find task " + taskId);
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
        // Convert fragment params to object.
        var fragmentQuery = URI.parseQuery(location.hash.replace('#', '')); // "http://example.org/#!/foo/bar/baz.html");

        if (fragmentQuery.tab == "workers") {
            switchTab("workerList");
        } else if (fragmentQuery.tab == "resources") {
            expandResources(fragmentQuery.resources);
            switchTab("resourceList");
        } else if (fragmentQuery.tab == "graph") {
            var taskId = fragmentQuery.taskId;
            var hideDone = fragmentQuery.hideDone === '1' ? true : false;

            // Populate fields with values from hash.
            $('#hideDoneCheckbox').prop('checked', hideDone);
            $("#invertCheckbox").prop('checked', fragmentQuery.invert === '1' ? true : false);
            $("#js-task-id").val(fragmentQuery.taskId);

            // Empty errors.
            $("#searchError").empty();
            $("#searchError").removeClass();

            var visType = fragmentQuery.visType || VISTYPE_DEFAULT;
            if (taskId) {
                var depGraphCallback = makeGraphCallback(visType, taskId, paint);

                if (fragmentQuery.invert) {
                    luigi.getInverseDependencyGraph(taskId, depGraphCallback, !hideDone);
                } else {
                    luigi.getDependencyGraph(taskId, depGraphCallback, !hideDone);
                }
            }
            updateVisType(visType);
            initVisualisation(visType);
            switchTab("dependencyGraph");
        } else {
            // Tasks tab.

            // Populate fields with values from hash.
            if (fragmentQuery.length) {
                $('select[name=taskTable_length]').val(fragmentQuery.length);
            }
            $("#serverSideCheckbox").prop('checked', fragmentQuery.filterOnServer === '1' ? true : false);
            dt.search(fragmentQuery.search__search);

            $('#familySidebar li').removeClass('active');
            $('#familySidebar li .badge').removeClass('bg-green');
            if (fragmentQuery.family) {
                family_item = $('#familySidebar li[data-task="' + fragmentQuery.family + '"]');
                family_item.addClass('active');
                family_item.find('.badge').addClass('bg-green');
                filterByTaskFamily(fragmentQuery.family, dt);
            }

            if (fragmentQuery.statuses) {
                var statuses = JSON.parse(fragmentQuery.statuses);
                $.each(statuses, function (status) {
                    toggleInfoBox($('#' + statuses[status] + '_info')[0], true);
                });
                filterByCategory(dt, statuses);
            }

            if (fragmentQuery.order) {
                dt.order([fragmentQuery.order.split(',')]);
            }
            dt.draw();
            switchTab("taskList");
        }
    }

    function bindGraphEvents() {
        var fragmentQuery = URI.parseQuery(location.hash.replace('#', ''));
        var visType = fragmentQuery.visType;
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
                    fragmentQuery['taskId'] = taskId;
                    window.location.href = 'index.html#' + URI.buildQuery(fragmentQuery);
                }
            });
        }
        else {
            $(".graph-node-a").click(function(event) {
                var taskId = $(this).attr("data-task-id");
                var status = $(this).attr("data-task-status");
                if (status == "FAILED") {
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

        $('#serverSideCheckbox').click(function(e) {
            e.preventDefault();
            changeState('filterOnServer', this.checked ? '1' : null);
            updateTasks();
        });

        $("#invertCheckbox").click(function(e) {
            e.preventDefault();
            changeState('invert', this.checked ? '1' : null);
        });

        $('#hideDoneCheckbox').click(function(e) {
            // Copy checkbox value to hash.
            e.preventDefault();
            changeState('hideDone', this.checked ? '1' : null);
        });
        $("a[href=#list]").click(function() { location.hash=""; });
        $("#loadTaskForm").submit(function(event) {
            event.preventDefault();
            var taskId = $(this).find("input").val();
            changeState('taskId', taskId.length > 0 ? taskId : null);
        });

        $('.info-box').on('click', function () {
            toggleInfoBox(this);
            filterByCategory(dt);
        });

        $('input[name=vis-type]').on('change', function () {
            changeState('visType', $(this).val());
        });

        /*
          Note: The #filter-input element is used by LuigiAPI to constrain requests to the server.
          When the accompanying button is pressed we force a reload.
         */
        $('#serverSide').on('change', 'label', function () {
            updateTasks();
        });
    }

    function asingInput(worker, id){
        if (worker[id].deps.length > 0) {
            //console.log(worker[id].deps);
            return worker[id].deps;
        }
    }

    function getDurations(tasks, listId){
        var durations = {};
        for (var i = 0; i < listId.length; i++) {
            for (var j = 0; j < tasks.length; j++) {
                if (listId[i] === tasks[j].taskId) {
                    // The duration of the task from when it started running to when it finished.
                    var finishTime = new Date(tasks[j].last_updated*1000);
                    var startTime = new Date(tasks[j].time_running*1000);
                    durations[listId[i]] = new Date(finishTime - startTime);
                }
            }
        }
        return durations;
    }

    function getParam(tasks, id){
        for (var i = 0; i < tasks.length; i++) {
            if (tasks[i].taskId === id) {
                return tasks[i].worker_running;
            }
        }
    }

    function getStatusTasks(tasks){
        var status;
        for (var i = 0; i < tasks.length; i++) {
            if (tasks[i].status === "DONE") {
                status = true;
            } else {
                return false;
            }
        }
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
                                var durations = getDurations(tasks, task.inputQueue);
                                var duration = durations[task.inputQueue[i]];
                                var oneDayInMilliseconds = 24 * 60 * 60 * 1000;
                                var durationLabel;
                                if (duration.getTime() < oneDayInMilliseconds) {
                                    // Label task duration in stripped ISO format (hh:mm:ss.f)
                                    durationLabel = duration.toISOString().substr(11, 12);
                                } else {
                                    durationLabel = "> 24h";
                                }
                                g.setEdge(task.inputQueue[i], task.taskId, {
                                    label: durationLabel,
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
                .attr("title", function(v) { return styleTooltip(v, getParam(tasks, v)); })
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
                'BATCH_RUNNING',
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
            taskCount = tasks[0] === -1 ? 'unknown' : tasks[0];
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

        // Prepare graphPlaceholder for D3 code
        if (newVisType == 'd3') {
            $('#graphPlaceholder').empty();
            $('#graphPlaceholder').html('<div class="live map"><svg width="100%" height="100%" id="mysvg"><g/></svg></div>');
        }
        else {
            $('#graphPlaceholder').empty();
            var graph = new Graph.DependencyGraph($("#graphPlaceholder")[0]);
            $("#graphPlaceholder")[0].graph = graph;
        }
    }

    function updateTasks() {
        $('.status-info .info-box-number').text('?');
        $('.status-info i.fa').removeClass().addClass('fa fa-spinner fa-pulse');

        var ajax1 = luigi.getRunningTaskList(function(runningTasks) {
            updateTaskCategory(dt, 'RUNNING', runningTasks);
        });

        var ajax2 = luigi.getBatchRunningTaskList(function(batchRunningTasks) {
            updateTaskCategory(dt, 'BATCH_RUNNING', batchRunningTasks);
        });

        var ajax3 = luigi.getFailedTaskList(function(failedTasks) {
            updateTaskCategory(dt, 'FAILED', failedTasks);
        });

        var ajax4 = luigi.getUpstreamFailedTaskList(function(upstreamFailedTasks) {
            updateTaskCategory(dt, 'UPSTREAM_FAILED', upstreamFailedTasks);
        });

        var ajax5 = luigi.getDisabledTaskList(function(disabledTasks) {
            updateTaskCategory(dt, 'DISABLED', disabledTasks);
        });

        var ajax6 = luigi.getUpstreamDisabledTaskList(function(upstreamDisabledTasks) {
            updateTaskCategory(dt, 'UPSTREAM_DISABLED', upstreamDisabledTasks);
        });

        var ajax7 = luigi.getPendingTaskList(function(pendingTasks) {
            updateTaskCategory(dt, 'PENDING', pendingTasks);
        });

        var ajax8 = luigi.getDoneTaskList(function(doneTasks) {
            updateTaskCategory(dt, 'DONE', doneTasks);
        });

        $.when(ajax1, ajax2, ajax3, ajax4, ajax5, ajax6, ajax7, ajax8).done(function () {
            dt.draw();

            $('.sidebar').html(renderSidebar(dt.column(1).data()));
            var selectedFamily = $('.sidebar-menu').find('li[data-task="' + currentFilter.taskFamily + '"]')[0];
            selectSidebarItem(selectedFamily);

            if (selectedFamily) {
                var selectedUl = $(selectedFamily).parent();
                selectedUl.show();
                selectedUl.prev().addClass('expanded');
            } else {
                var others = $('.sidebar-folder:contains(Others)')
                others.addClass('expanded')
                others.next().show()
            }

            $('.sidebar-menu').on('click', 'li:not(.sidebar-folder)', function (e) {
                e.stopPropagation();
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

            $('.sidebar-menu').on('click', '.sidebar-folder', function () {
                const ul = this.nextElementSibling;
                $(ul).slideToggle()
                this.classList.toggle('expanded')
            })

            $('#clear-task-filter').on('click', function () {
                filterByTaskFamily("", dt);
            });

            if ($.isEmptyObject(missingCategories)) {
                $('#warnings').html('');
            }
            else {
                $('#warnings').html(renderWarnings());
            }

            processHashChange();
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

    /**
     * Updates the number of worker processes of a worker
     * @param worker: the id of the worker
     * @param n: the number of processes to set
     */
    function updateWorkerProcesses(worker, n) {
        n = Math.max(1, n);

        // the spinner is just for visual feedback
        var $label = $('#workerList').find('#label-n-workers[data-worker="' + worker + '"]');
        $label.html('<i class="fa fa-spinner fa-spin" aria-hidden="true"></i>');

        luigi.setWorkerProcesses(worker, n, function() {
            $label.text(n);
        });
    }

    /**
     * Updates the number of units of a given resource available in the scheduler
     * @param resource: the name of the resource
     * @param n: the number of units to set the resource limit to
     */
    function updateResourceCount(resource, n) {
        var progressBar = $('#' + resource + '-resource-box .progress-bar');
        var used = /(\S+)\//.exec(progressBar.text())[1];
        nVal = parseInt(n);
        if (isNaN(nVal) || nVal < 0) {
            return;
        }
        usedVal = parseInt(used);
        width = Math.floor(100 * usedVal / nVal);
        if (width < 0) {
            width = 0;
        }
        if (width > 100) {
            width = 100;
        }
        luigi.updateResource(resource, n, function() {
            progressBar.text(usedVal + '/' + nVal);
            progressBar.attr('style', 'width: ' + width + '%');
        });
    }

    /**
     * Returns the current units of a resource used
     * @param resource: the name of the resource
     */
    function currentResourceCount(resource) {
        var progressBar = $('#' + resource + '-resource-box .progress-bar');
        var count = /\/(\S+)/.exec(progressBar.text())[1];
        return parseInt(count);
    }

    function changeState(key, value) {
        var fragmentQuery = URI.parseQuery(location.hash.replace('#', ''));
        if (value) {
            fragmentQuery[key] = value;
        } else {
            delete fragmentQuery[key];
        }
        location.hash = '#' + URI.buildQuery(fragmentQuery);
    }

   function expandedResources() {
        return $('.resource-box.in').toArray().map(function (val) { return val.dataset.resource; });
    }

    function expandResources(resources) {
        if (resources === undefined) {
            resources = [];
        } else {
            resources = JSON.parse(resources);
        }
        $('.resource-box').each(function (i, item) {
            if (resources.indexOf(item.dataset.resource) === -1) {
                $(item).collapse('hide');
            } else {
                $(item).collapse('show');
            }
        });
    }

    /**
     * Create the pause/unpause toggle
     */
    function createPauseToggle(checked) {
        var check = checked ? " checked" : "";
        var html = $('<input id="pause" type="checkbox"' + check + ' data-toggle="toggle">');
        $('#pause-form').append(html);
        $('#pause').bootstrapToggle({
            on: 'Running',
            off: 'Paused',
            onstyle: 'success',
            offstyle: 'danger'
        });
        $('#pause').change(function() {
            if (this.checked) {
                luigi.unpause();
            } else {
                luigi.pause();
            }
        })
    }

    $(document).ready(function() {
        loadTemplates();

        luigi.hasTaskHistory(function(hasTaskHistory) {
            if (hasTaskHistory) {
                $('#topNavbar').append(renderTemplate('topNavbarItem', {
                    label: "History",
                    href: "../../history",
                }).children()[0]);
            }
        });

        luigi.isPauseEnabled(function(enabled) {
            if (enabled) {
                luigi.isPaused(createPauseToggle);
            }
        });

        luigi.getWorkerList(function(workers) {
            $("#workerList").append(renderWorkers(workers));

            $('.worker-table tbody').on('click', 'td .statusMessage', function() {
                var data = $(this).data();
                showStatusMessage(data);
            });

            $('.worker-table tbody').on('click', 'td .schedulerMessage', function() {
                var data = $(this).data();
                showSchedulerMessageModal(data);
            });
        });

        luigi.getResourceList(function(resources) {
            $("#resourceList").append(renderResources(resources));
            expandResources(URI.parseQuery(location.hash.replace('#', '')).resources);
            $('.resources-collapse').click(function (e) {
                e.preventDefault();
                var collapse_block = $(this.dataset.target);
                if (collapse_block.hasClass('collapsing')) {
                    return;
                }
                var resource = collapse_block.attr('data-resource');
                var resourceList = expandedResources();
                var resourceIdx = resourceList.indexOf(resource);
                if (resourceIdx === -1) {
                    resourceList.push(resource);
                } else {
                    resourceList.splice(resourceIdx, 1);
                }
                changeState('resources', resourceList.length > 0 ? JSON.stringify(resourceList) : null);
                collapse_block.collapse('toggle');
            });
        });

        dt = $('#taskTable').DataTable({
            stateSave: true,
            stateSaveCallback: function(settings, data) {
                // Save data table state to browser's hash.
                var state = URI.parseQuery(location.hash.replace('#', ''));

                if (data.search.search) {
                    state.search__search = data.search.search;
                } else {
                    delete state.search__search;
                }

                var family_search = data.columns[1].search.search;
                if (family_search) {
                    state.family = family_search.substring(1, family_search.length - 1);
                } else {
                    delete state.family;
                }

                if (currentFilter.taskCategory.length > 0) {
                    state.statuses = JSON.stringify(currentFilter.taskCategory);
                } else {
                    delete state.statuses;
                }

                if (data.order && data.order.length) {
                    state.order = '' + data.order[0][0] + ',' + data.order[0][1];
                }

                if (data.length && data.length !== 10) {
                    // Keep in hash only if length is not default.
                    state.length = data.length;
                } else {
                    delete state.length;
                }

                if (state.filterOnServer) {
                    state.filterOnServer = '1';
                }
                location.hash = '#' + URI.buildQuery(state);
            },
            stateLoadCallback: function(settings) {
                // Restore datatable state from browser's hash.
                var fragmentQuery = URI.parseQuery(location.hash.replace('#', ''));

                var order = [];
                if (fragmentQuery.order) {
                    order = [fragmentQuery.order.split(',')];
                }

                var family_search = {};
                if (fragmentQuery.family) {
                    family_search = {'search': '^' + fragmentQuery.family + '$', 'regex': true};
                }

                var status_search = {};
                if (fragmentQuery.statuses) {
                    var statuses = JSON.parse(fragmentQuery.statuses);
                    currentFilter.taskCategory = statuses;
                    status_search = {'search': categoryQuery(statuses), 'regex': true};
                }

                // Prepare state for datatable.
                var o = {
                    order: order,                 // Table rows order.
                    length: fragmentQuery.length, // Entries on page.
                    start: 0,                     // Pagination initial page.
                    time: new Date().getTime(),   // Current time to help datatable.js to handle asynchronous.
                    columns: [
                        {visible: true, search: status_search},
                        {visible: true, search: family_search},  // Name column
                        {visible: true, search: {}},  // Details column
                        {visible: true, search: {}},  // Priority column
                        {visible: true, search: {}},  // Time column
                        {visible: true, search: {}}   // Actions column
                    ],
                    // Search input state.
                    search: {
                        caseInsensitive: true,
                        search: fragmentQuery.search__search
                    }
                };

                return o;
            },
            dom: 'l<"#serverSide">frtip',
            language: {
                search: 'Filter table:'
            },
            columns: [
                {
                    data: 'category',
                    render: function (data, type, row) {
                        return taskCategoryIcon(data) + ' ' + data;
                    }
                },
                {data: 'taskName'},
                {
                    data: 'taskParams',
                    render: function(data, type, row) {
                        var params = JSON.parse(data);
                        if (row.resources !== '{}') {
                            return '<div>' + renderParams(params) + '</div><div>' + row.resources + '</div>';
                        } else {
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
                        return Mustache.render(templates.actionsTemplate, row);
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

        processHashChange();
        updateTasks();
        bindListEvents();

        $('#taskTable tbody').on('click', 'td.details-control .showError', function () {
            var tr = $(this).closest('tr');
            var row = dt.row( tr );
            var data = row.data();
            luigi.getErrorTrace(data.taskId, function(error) {
                showErrorTrace(error);
            });
        } );

        $('#taskTable tbody').on('click', 'td.details-control .forgiveFailures', function (ev) {
            var that = $(this);
            var tr = that.closest('tr');
            var row = dt.row( tr );
            var data = row.data();
            luigi.forgiveFailures(data.taskId, function(data) {
                if (ev.altKey) {
                    updateTasks(); // update may not be cheap
                } else {
                    that.tooltip('hide');
                    that.remove();
                }
            });
        } );

        $('#taskTable tbody').on('click', 'td.details-control .markAsDone', function (ev) {
            var that = $(this);
            var tr = that.closest('tr');
            var row = dt.row( tr );
            var data = row.data();
            luigi.markAsDone(data.taskId, function(data) {
                if (ev.altKey) {
                    updateTasks(); // update may not be cheap
                } else {
                    that.tooltip('hide');
                    that.remove();
                }
            });
        } );

        $('#taskTable tbody').on('click', 'td.details-control .re-enable-button', function (ev) {
            var that = $(this);
            luigi.reEnable(that.attr("data-task-id"), function(data) {
                if (ev.altKey) {
                    updateTasks(); // update may not be cheap
                } else {
                    that.tooltip('hide');
                    that.remove();
                }
            });
        });

        $('#taskTable tbody').on('click', 'td.details-control .statusMessage', function () {
            var data = $(this).data();
            showStatusMessage(data);
        });

        $('#taskTable tbody').on('click', 'td.details-control .schedulerMessage', function () {
            var data = $(this).data();
            showSchedulerMessageModal(data);
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
            var box = triggerButton.parents('.box').addClass('box-solid box-default');

            // remove the worker tools
            box.find('.box-tools').remove();
        });

        $('#workerList').on('click', '#btn-increment-workers', function($event) {
            var worker = $(this).data("worker");
            var $label = $('#workerList').find('#label-n-workers[data-worker="' + worker + '"]');
            var n = parseInt($label.text());
            if (!isNaN(n)) {
                updateWorkerProcesses(worker, n + 1);
            }
            $event.preventDefault();
        });

        $('#workerList').on('click', '#btn-decrement-workers', function($event) {
            var worker = $(this).data("worker");
            var $label = $('#workerList').find('#label-n-workers[data-worker="' + worker + '"]');
            var n = parseInt($label.text());
            if (!isNaN(n)) {
                updateWorkerProcesses(worker, n - 1);
            }
            $event.preventDefault();
        });

        $('#workerList').on('show.bs.modal', '#setWorkersModal', function($event) {
            $('#setWorkersButton').data('worker', $($event.relatedTarget).data('worker'));
            var $input = $(this).find('#setWorkersInput').on('keypress', function($event) {
                if (event.keyCode == 13) {
                    $('#workerList').find('#setWorkersButton').trigger('click');
                }
                $event.stopPropagation();
            });
            setTimeout(function() {
                $input.focus();
            }.bind(this), 600);
        });

        $('#workerList').on('hidden.bs.modal', '#setWorkersModal', function() {
            $(this).find('#setWorkersInput').off('keypress').val('');
        });

        $('#workerList').on('click', '#setWorkersButton', function($event) {
            var worker = $(this).data('worker');
            var n = parseInt($("#setWorkersInput").val());
            if (!isNaN(n)) {
                updateWorkerProcesses(worker, n);
            }
            $event.preventDefault();
        });

        $('#resourceList').on('click', '.btn-increment-resources', function($event) {
            $event.preventDefault();
            var resource = $(this).data('resource');
            var count = currentResourceCount(resource);
            updateResourceCount(resource, count + 1);
        });

        $('#resourceList').on('click', '.btn-decrement-resources', function($event) {
            $event.preventDefault();
            var resource = $(this).data('resource');
            var count = currentResourceCount(resource);
            updateResourceCount(resource, count - 1);
        });

        $('#resourceList').on('show.bs.modal', '#setResourcesModal', function($event) {
            $('#setResourcesButton').data('resource', $($event.relatedTarget).data('resource'));
            var $input = $(this).find('#setResourcesInput').on('keypress', function($event) {
                if (event.keyCode == 13) {
                    $('#resourceList').find('#setResourcesButton').trigger('click');
                }
                $event.stopPropagation();
            });
            setTimeout(function() {
                $input.focus();
            }.bind(this), 600);
        });

        $('#resourceList').on('hidden.bs.modal', '#setResourcesModal', function() {
            $(this).find('#setResourcesInput').off('keypress').val('');
        });

        $('#resourceList').on('click', '#setResourcesButton', function($event) {
            var resource = $(this).data('resource');
            var n = parseInt($("#setResourcesInput").val());
            updateResourceCount(resource, n);
            $event.preventDefault();
        });
        $('.js-nav-link').click(function(e) {
            // User followed tab from navigation link. Copy state from fields to hash.
            e.preventDefault();
            var state = {};
            var tabId = $(this).attr('data-tab');

            if (tabId == 'taskList') {
                var order = dt.order();
                var search = dt.search();
                state.tab = 'tasks';

                if ($('select[name=taskTable_length]').val() !== '10') {
                    // Add length to hash only if the value is not default.
                    state.length = $('select[name=taskTable_length]').val();
                }

                if ($('#serverSideCheckbox').is(':checked')) {
                    state.filterOnServer = '1';
                }

                var family = $('#familySidebar li.active').attr('data-task');
                if (family) {
                    state.family = family;
                } else {
                    delete state.family;
                }

                if (currentFilter.taskCategory.length > 0) {
                    state.statuses = JSON.stringify(currentFilter.taskCategory);
                } else {
                    delete state.statuses;
                }

                if (search) {
                    state.search__search = search;
                }

                if (order.length > 0) {
                    state.order = '' + order[0][0] + ',' + order[0][1];
                }

            } else if (tabId == 'dependencyGraph') {
                state.tab = 'graph';

                // Get state from fields.

                if ($('#hideDoneCheckbox').is(':checked')) {
                    state.hideDone = '1';
                }

                if ($('#idTaskForm input.search-query').val()) {
                    state.taskId = $('#idTaskForm input.search-query').val();
                }

                if ($('#invertCheckbox').is(':checked')) {
                    state.invert = '1';
                }

                state.visType = $('input[name=vis-type]:checked').val();
            } else if (tabId == 'workerList') {
                state.tab = 'workers';
            } else if (tabId == 'resourceList') {
                state.resources = JSON.stringify(expandedResources());
                state.tab = 'resources';
            }

            location.hash = '#' + URI.buildQuery(state);
        });

        processHashChange();
    });
}
