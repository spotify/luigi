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
    }

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

    LuigiAPI.prototype.getRunningTaskList = function(callback) {
        return jsonRPC(this.urlRoot + "/task_list", {status: "RUNNING", upstream_status: "", search: searchTerm()}, function(response) {
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
    }

    return LuigiAPI;
})();
