var LuigiAPI = (function() {
    function LuigiAPI (urlRoot) {
        this.urlRoot = urlRoot;
    }

    function flatten(response, rootId) {
        var flattened = [];
        // Make the requested taskId the first in the list
        if (rootId && response[rootId]) {
            var rootNode = response[rootId];
            rootNode.taskId=rootId;
            flattened.push(rootNode);
            delete response[rootId];
        }
        $.each(response, function(key, value) {
            value.taskId = key;
            flattened.push(value);
        });
        return flattened;
    }

    function flatten_running(response) {
        $.each(response, function(key, value) {
            value.running = flatten(value.running);
        });
        return response;
    }

    function jsonRPC(url, paramObject, callback) {
        return $.ajax(url, {
            data: {data: JSON.stringify(paramObject)},
            method: "GET",
            success: callback,
            dataType: "json"
        });
    }

    function searchTerm() {
        // FIXME : leaky API.  This shouldn't rely on the DOM.
        if ($('#serverSideCheckbox')[0].checked) {
            return $('#taskTable_filter').find('input').val();
        }
        else {
            return '';
        }
    }

    LuigiAPI.prototype.getDependencyGraph = function (taskId, callback, include_done) {
        return jsonRPC(this.urlRoot + "/dep_graph", {task_id: taskId, include_done: include_done}, function(response) {
            callback(flatten(response.response, taskId));
        });
    };

    LuigiAPI.prototype.getInverseDependencyGraph = function (taskId, callback, include_done) {
        return jsonRPC(this.urlRoot + "/inverse_dep_graph", {task_id: taskId, include_done: include_done}, function(response) {
            callback(flatten(response.response, taskId));
        });
    };

    LuigiAPI.prototype.forgiveFailures = function (taskId, callback) {
        return jsonRPC(this.urlRoot + "/forgive_failures", {task_id: taskId}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.markAsDone = function (taskId, callback) {
        return jsonRPC(this.urlRoot + "/mark_as_done", {task_id: taskId}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getFailedTaskList = function(callback) {
        return jsonRPC(this.urlRoot + "/task_list", {status: "FAILED", upstream_status: "", search: searchTerm()}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getUpstreamFailedTaskList = function(callback) {
        return jsonRPC(this.urlRoot + "/task_list", {status: "PENDING", upstream_status: "UPSTREAM_FAILED", search: searchTerm()}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getDoneTaskList = function(callback) {
        return jsonRPC(this.urlRoot + "/task_list", {status: "DONE", upstream_status: "", search: searchTerm()}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.reEnable = function(taskId, callback) {
        return jsonRPC(this.urlRoot + "/re_enable_task", {task_id: taskId}, function(response) {
            callback(response.response);
        });
    };

    LuigiAPI.prototype.getErrorTrace = function(taskId, callback) {
        return jsonRPC(this.urlRoot + "/fetch_error", {task_id: taskId}, function(response) {
            callback(response.response);
        });
    };

    LuigiAPI.prototype.getTaskStatusMessage = function(taskId, callback) {
        return jsonRPC(this.urlRoot + "/get_task_status_message", {task_id: taskId}, function(response) {
            callback(response.response);
        });
    };

    LuigiAPI.prototype.getTaskProgressPercentage = function(taskId, callback) {
        return jsonRPC(this.urlRoot + "/get_task_progress_percentage", {task_id: taskId}, function(response) {
            callback(response.response);
        });
    };

    LuigiAPI.prototype.getRunningTaskList = function(callback) {
        return jsonRPC(this.urlRoot + "/task_list", {status: "RUNNING", upstream_status: "", search: searchTerm()}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getBatchRunningTaskList = function(callback) {
        return jsonRPC(this.urlRoot + "/task_list", {status: "BATCH_RUNNING", upstream_status: "", search: searchTerm()}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getPendingTaskList = function(callback) {
        return jsonRPC(this.urlRoot + "/task_list", {status: "PENDING", upstream_status: "", search: searchTerm()}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getDisabledTaskList = function(callback) {
        jsonRPC(this.urlRoot + "/task_list", {status: "DISABLED", upstream_status: "", search: searchTerm()}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getUpstreamDisabledTaskList = function(callback) {
        jsonRPC(this.urlRoot + "/task_list", {status: "PENDING", upstream_status: "UPSTREAM_DISABLED", search: searchTerm()}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getWorkerList = function(callback) {
        jsonRPC(this.urlRoot + "/worker_list", {}, function(response) {
            callback(flatten_running(response.response));
        });
    };

    LuigiAPI.prototype.getResourceList = function(callback) {
        jsonRPC(this.urlRoot + "/resource_list", {}, function(response) {
            callback(flatten_running(response.response));
        });
    };

    LuigiAPI.prototype.disableWorker = function(workerId) {
        jsonRPC(this.urlRoot + "/disable_worker", {'worker': workerId});
    };

    LuigiAPI.prototype.setWorkerProcesses = function(workerId, n, callback) {
        var data = {worker: workerId, n: n};
        jsonRPC(this.urlRoot + "/set_worker_processes", data, function(response) {
            callback();
        });
    };

    LuigiAPI.prototype.sendSchedulerMessage = function(workerId, taskId, content, callback) {
        var data = {worker: workerId, task: taskId, content: content};
        jsonRPC(this.urlRoot + "/send_scheduler_message", data, function(response) {
            if (callback) {
                callback(response.response.message_id);
            }
        });
    };

    LuigiAPI.prototype.getSchedulerMessageResponse = function(taskId, messageId, callback) {
        var data = {task_id: taskId, message_id: messageId};
        jsonRPC(this.urlRoot + "/get_scheduler_message_response", data, function(response) {
            callback(response.response.response);
        });
    };

    LuigiAPI.prototype.isPauseEnabled = function(callback) {
        jsonRPC(this.urlRoot + '/is_pause_enabled', {}, function(response) {
            callback(response.response.enabled);
        });
    };

    LuigiAPI.prototype.hasTaskHistory = function(callback) {
        jsonRPC(this.urlRoot + '/has_task_history', {}, function(response) {
            callback(response.response);
        });
    };

    LuigiAPI.prototype.pause = function() {
        jsonRPC(this.urlRoot + '/pause');
    };

    LuigiAPI.prototype.unpause = function() {
        jsonRPC(this.urlRoot + '/unpause');
    };

    LuigiAPI.prototype.isPaused = function(callback) {
        jsonRPC(this.urlRoot + "/is_paused", {}, function(response) {
            callback(!response.response.paused);
        });
    };

    LuigiAPI.prototype.updateResource = function(resource, n, callback) {
        var data = {'resource': resource, 'amount': n};
        jsonRPC(this.urlRoot + "/update_resource", data, function(response) {
            callback();
        });
    };

    return LuigiAPI;
})();
