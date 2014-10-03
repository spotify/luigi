function visualiserApp(luigi) {
    var templates = {};
    var invertDependencies = false;

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
        var taskIdParts = /([A-Za-z0-9_]*)\((.*)\)/.exec(task.taskId);
        var taskName = taskIdParts[1];
        var taskParams = taskIdParts[2];
        var displayTime = new Date(Math.floor(task.start_time*1000)).toLocaleString();
        if (task.status == "RUNNING" && "time_running" in task) {
            var current_time = new Date().getTime()
            var minutes_running = Math.round((current_time - task.time_running * 1000) / 1000 / 60)
            displayTime += " | " + minutes_running + " minutes"
            if ("worker_running" in task) {
              displayTime += " (" + task.worker_running + ")"
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
            error: task.status == "FAILED"
        };
    }

    function indexByProperty(tasks, fieldName) {
        indexedTasks = {};
        $.each(tasks, function(i, task) {
            if (!indexedTasks.hasOwnProperty(task[fieldName])) {
                indexedTasks[task[fieldName]] = [];
            }
            indexedTasks[task[fieldName]].push(task);
        });
        return indexedTasks;
    }

    function entryList(object) {
        var list = [];
        $.each(object, function(key, value) {
            list.push({key: key, value:value});
        });
        return list;
    }

    function renderTasks(tasks) {
        var displayTasks = tasks.map(taskToDisplayTask);
        displayTasks.sort(function(a,b) { return b.displayTimestamp - a.displayTimestamp; });
        var tasksByFamily = entryList(indexByProperty(displayTasks, "taskName"));
        tasksByFamily.sort(function(a,b) { return a.key.localeCompare(b.key); });
        return renderTemplate("rowTemplate", {tasks: tasksByFamily});
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

    function processHashChange() {
        var hash = location.hash;
        if (hash) {
            var taskId = hash.substr(1);
            $("#graphContainer").hide();
            $("#graphPlaceholder svg").empty();
            $("#searchError").empty();
            $("#searchError").removeClass();
            if (taskId != "g") {
                depGraphCallback = function(dependencyGraph) {
                    $("#graphPlaceholder svg").empty();
                    $("#searchError").empty();
                    $("#searchError").removeClass();
                    if(dependencyGraph.length > 0) {
                      $("#dependencyTitle").text(taskId);
                      $("#graphPlaceholder").get(0).graph.updateData(dependencyGraph);
                      $("#graphContainer").show();
                      bindGraphEvents();
                    } else {
                      $("#searchError").addClass("alert alert-error")
                      $("#searchError").append("Couldn't find task " + taskId)
                    }
                }
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
            processHashChange();
        });
        $("a[href=#list]").click(function() { location.hash=""; });
        $("#loadTaskForm").submit(function(event) {
            event.preventDefault();
            location.hash = $(this).find("input").val();
        });
    }

    function bindTaskEvents(id, expand) {
        $(id + " [data-action=expandTaskRows]").click(function(event) {
            event.preventDefault();
            var icon = $(this).find("span");
            if (icon.hasClass("icon-plus")) {
                icon.removeClass("icon-plus");
                icon.addClass("icon-minus");
            } else {
                icon.removeClass("icon-minus");
                icon.addClass("icon-plus");
            }
            var taskRows = $(this).closest(".taskFamily").find(".taskRows").slideToggle("fast");
        });
        if (expand) {
            $(id + " [data-action=expandTaskRows]").click();
        }
        $(id + " .error-trace-button").click(function() {
            luigi.getErrorTrace($(this).attr("data-task-id"), function(error) {
               showErrorTrace(error);
            });
        });
    }

    $(document).ready(function() {
        loadTemplates();

        luigi.getRunningTaskList(function(runningTasks) {
            $("#runningTasks").append(renderTasks(runningTasks));
            bindTaskEvents("#runningTasks", true);
        });

        luigi.getFailedTaskList(function(failedTasks) {
            $("#failedTasks").append(renderTasks(failedTasks));
            bindTaskEvents("#failedTasks");
        });

        luigi.getUpstreamFailedTaskList(function(upstreamFailedTasks) {
            $("#upstreamFailedTasks").append(renderTasks(upstreamFailedTasks));
            bindTaskEvents("#upstreamFailedTasks");
        });

        luigi.getPendingTaskList(function(pendingTasks) {
            $("#pendingTasks").append(renderTasks(pendingTasks));
            bindTaskEvents("#pendingTasks");
        });

        luigi.getDoneTaskList(function(doneTasks) {
            $("#doneTasks").append(renderTasks(doneTasks));
            bindTaskEvents("#doneTasks");
        });

        bindListEvents();

        var graph = new Graph.DependencyGraph($("#graphPlaceholder")[0]);
        $("#graphPlaceholder")[0].graph = graph;
        processHashChange();
    });
}
