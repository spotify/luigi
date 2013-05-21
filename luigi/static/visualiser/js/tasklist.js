(function(luigi) {
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
            trackingUrl: task.trackingUrl,
            status: task.status,
            graph: task.status == "PENDING"
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
        var tasksByFamily = entryList(indexByProperty(displayTasks, "taskName"));
        return renderTemplate("rowTemplate", {tasks: tasksByFamily});
    }

    function switchTab(tabId) {
        $(".tabButton").parent().removeClass("active");
        $(".tab-pane").removeClass("active");
        $("#"+tabId).addClass("active");
        $(".tabButton[data-tab="+tabId+"]").parent().addClass("active");
    }

    function processHashChange() {
        var hash = location.hash;
        if (hash) {
            taskId = hash.substr(1);
            $("#graphContainer").hide();
            if (taskId != "g") {
                luigi.getDependencyGraph(taskId, function(dependencyGraph) {
                    $("#dependencyTitle").text(taskId);
                    $("#d3Container").get(0).graph.updateData(dependencyGraph);
                    $("#graphContainer").show();
                });
            }
            switchTab("dependencyGraph");
        } else {
            switchTab("taskList");
        }
    }

    function bindUserEvents() {
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

    }

    $(document).ready(function() {
        loadTemplates();
        luigi.getFailedTaskList(function(failedTasks) {
            luigi.getUpstreamFailedTaskList(function(upstreamFailedTasks) {
                $("#failedTasks").append(renderTasks(failedTasks));
                $("#upstreamFailedTasks").append(renderTasks(upstreamFailedTasks));
                bindUserEvents();
            });
        });
        var graph = new Graph.DependencyGraph($("#d3Container")[0]);
        $("#d3Container")[0].graph = graph;
        processHashChange();
    });
})(new LuigiAPI("/api"));
