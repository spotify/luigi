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

    LuigiAPI.prototype.getDependencyGraph = function (taskId, callback) {
        jsonRPC(this.urlRoot + "/dep_graph", {task_id: taskId}, function(response) {
            callback(flatten(response.response, taskId));
        });
    };

    LuigiAPI.prototype.getInverseDependencyGraph = function (taskId, callback) {
        jsonRPC(this.urlRoot + "/inverse_dep_graph", {task_id: taskId}, function(response) {
            callback(flatten(response.response, taskId));
        });
    }

    LuigiAPI.prototype.getFailedTaskList = function(callback) {
        jsonRPC(this.urlRoot + "/task_list", {status: "FAILED", upstream_status: ""}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getUpstreamFailedTaskList = function(callback) {
        jsonRPC(this.urlRoot + "/task_list", {status: "PENDING", upstream_status: "UPSTREAM_FAILED"}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getDoneTaskList = function(callback) {
        jsonRPC(this.urlRoot + "/task_list", {status: "DONE", upstream_status: ""}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.reEnable = function(taskId, callback) {
        jsonRPC(this.urlRoot + "/re_enable_task", {task_id: taskId}, function(response) {
            callback(response.response);
        });
    };

    LuigiAPI.prototype.getErrorTrace = function(taskId, callback) {
        jsonRPC(this.urlRoot + "/fetch_error", {task_id: taskId}, function(response) {
            callback(response.response);
        });
    };

    LuigiAPI.prototype.getRunningTaskList = function(callback) {
        jsonRPC(this.urlRoot + "/task_list", {status: "RUNNING", upstream_status: ""}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getPendingTaskList = function(callback) {
        jsonRPC(this.urlRoot + "/task_list", {status: "PENDING", upstream_status: ""}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getDisabledTaskList = function(callback) {
        jsonRPC(this.urlRoot + "/task_list", {status: "DISABLED", upstream_status: ""}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getUpstreamDisabledTaskList = function(callback) {
        jsonRPC(this.urlRoot + "/task_list", {status: "PENDING", upstream_status: "UPSTREAM_DISABLED"}, function(response) {
            callback(flatten(response.response));
        });
    };

    LuigiAPI.prototype.getWorkerList = function(callback) {
        jsonRPC(this.urlRoot + "/worker_list", {}, function(response) {
            callback(flatten_running(response.response));
        });
    };

    return LuigiAPI;
})();
