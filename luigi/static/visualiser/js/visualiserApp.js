function visualiserApp(luigi) {
    var templates = {};

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
        var taskIdParts = /([A-Za-z]*)\((.*)\)/.exec(task.taskId);
        var taskName = taskIdParts[1];
        var taskParams = taskIdParts[2];
        var displayTime = new Date(Math.floor(task.start_time*1000)).toLocaleTimeString();
        return {
            taskId: task.taskId,
            taskName: taskName,
            taskParams: taskParams,
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
        var displayTasks = $.map(tasks, taskToDisplayTask);
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
                luigi.getDependencyGraph(taskId, function(dependencyGraph) {
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
                });
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
        $("[data-action=expandTaskRows]").click(function(event) {
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
        $(window).on('hashchange', processHashChange);
        $("a[href=#list]").click(function() { location.hash=""; });
        $("#loadTaskForm").submit(function(event) {
            event.preventDefault();
            location.hash = $(this).find("input").val();
        });
        $(".error-trace-button").click(function() {
            luigi.getErrorTrace($(this).attr("data-task-id"), function(error) {
               showErrorTrace(error);
            });
        });
    }

    $(document).ready(function() {
        loadTemplates();
        
        luigi.getFailedTaskList(function(failedTasks) {
            luigi.getUpstreamFailedTaskList(function(upstreamFailedTasks) {
                luigi.getRunningTaskList(function(runningTasks) {
                    luigi.getPendingTaskList(function(pendingTasks) {
                        luigi.getDoneTaskList(function(doneTasks) {
                            $("#failedTasks").append(renderTasks(failedTasks));
                            $("#upstreamFailedTasks").append(renderTasks(upstreamFailedTasks));
                            $("#runningTasks").append(renderTasks(runningTasks));
                            $("#pendingTasks").append(renderTasks(pendingTasks));
                            $("#doneTasks").append(renderTasks(doneTasks));
                            bindListEvents();
                        });
                    });
                });
            });
        });
        
        var graph = new Graph.DependencyGraph($("#graphPlaceholder")[0]);
        $("#graphPlaceholder")[0].graph = graph;
        processHashChange();
    });
}
