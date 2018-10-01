var page = require('webpage').create();
var system = require('system');

var tests = [];

/*
 * Parse command line to get Luigi scheduler URL
 */
if (system.args.length === 1) {
    console.log('Usage: phantom_test.js <scheduler-url>');
    phantom.exit();
}

var url = system.args[1];


/*
 * Minimal test framework
 */
function do_tests(page) {
    var ok = true;
    var retval;

    tests.forEach(function (spec) {
        var name = spec[0];
        var test_func = spec[1];

        retval = report(page.evaluate(test_func), name);
        ok = ok && retval;
    });

    return ok;
}

function report(retval, func_name) {

    if (retval === true) {
        console.log('[ OK ]  ' + func_name);
        return true;
    }
    else {
        console.log('[FAIL]  ' + func_name);
        console.log(retval);
        return false;
    }
}

phantom.onError = function(msg, trace) {
    var msgStack = ['PHANTOM ERROR: ' + msg];
    if (trace && trace.length) {
        msgStack.push('TRACE:');
        trace.forEach(function(t) {
            msgStack.push(' -> ' + (t.file || t.sourceURL) + ': ' + t.line
                                    + (t.function ? ' (in function ' + t.function +')' : ''));
        });
    }
    console.error(msgStack.join('\n'));
    phantom.exit(1);
};

page.onError = function(msg, trace) {

  var msgStack = ['ERROR: ' + msg];

  if (trace && trace.length) {
    msgStack.push('TRACE:');
    trace.forEach(function(t) {
      msgStack.push(' -> ' + t.file + ': ' + t.line
                    + (t.function ? ' (in function "' + t.function +'")' : ''));
    });
  }

  console.error(msgStack.join('\n'));

};

/**
 * def_test: define a test
 * @param test_name: Name of test
 * @param func: A function which will be evaluated within the page and should return
 *    true for success or any other value for failure.
 */
function def_test(test_name, func) {
    tests.push([test_name, func]);
}


/*
 * Test definitions
 */
def_test('failed_info_test', function () {
    var el = $('#FAILED_info .info-box-number')[0];
    if (el.textContent === "4") {
        return true;
    }
    else {
        return el.textContent;
    }
});

def_test('done_info_test', function () {
    var el = $('#DONE_info .info-box-number')[0];
    if (el.textContent === "68") {
        return true;
    }
    else {
        return el.textContent;
    }
});

def_test('upstream_failure_info_test', function () {
    var el = $('#UPSTREAM_FAILED_info .info-box-number')[0];
    if (el.textContent === '45') {
        return true;
    }
    else {
        return el.textContent;
    }
});


def_test('result_count_test', function () {
    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 117 entries/)) {
        return true;
    }
    else {
        return el.textContent;
    }
});

def_test('filtered_result_count_test1', function () {
    var ret;
    var target = $('ul.sidebar-menu li a').first();

    target.click();

    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 29 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    target.click();
    return ret;
});


def_test('filtered_result_count_test2', function () {
    var ret;
    var target = $('#FAILED_info').first();

    target.click();
    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 4 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    target.click();
    return ret;

});


def_test('filtered_result_count_test3', function () {
    var ret;
    var target = $('#PENDING_info').first();

    target.click();
    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 0 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    target.click();
    return ret;

});


def_test('filtered_result_count_test4', function () {
    var ret;
    var target = $('#RUNNING_info').first();

    target.click();
    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 0 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    target.click();
    return ret;

});


def_test('filtered_result_count_test5', function () {
    var ret;
    var target = $('#DONE_info').first();

    target.click();
    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 68 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    target.click();
    return ret;

});


def_test('filtered_result_count_test5', function () {
    var ret;
    var target = $('#DISABLED_info').first();

    target.click();
    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 0 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    target.click();
    return ret;

});

def_test('filtered_result_count_test5', function () {
    var ret;
    var target = $('#UPSTREAM_DISABLED_info').first();

    target.click();
    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 0 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    target.click();
    return ret;

});


def_test('filtered_result_count_test5', function () {
    var ret;
    var target = $('#UPSTREAM_FAILED_info').first();

    target.click();
    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 45 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    target.click();
    return ret;

});

def_test('searched_result_count_test1', function () {
    var ret;
    var dt = $('#taskTable').DataTable();

    dt.search('FailingMergeSort_1').draw();


    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 29 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    dt.search('').draw();
    return ret;

});

def_test('searched_result_count_test1', function () {
    var ret;
    var target = $('#serverSide label').first();
    var dt = $('#taskTable').DataTable();

    target.click();
    dt.search('FailingMergeSort_1').draw();


    var el = $('#taskTable_info')[0];
    if (el.textContent.match(/Showing \d+ to \d+ of 29 entries.*from 117 total entries/)) {
        ret = true;
    }
    else {
        ret = el.textContent;
    }

    target.click();
    dt.search('').draw();
    return ret;

});


page.open(url, function(status) {
    var ok;

    console.log("Loaded " + url + ", status: " + status);
    if(status === "success") {
        ok = do_tests(page);
    }
    console.log('RESULT: ' + ok);
    phantom.exit(ok === true ? 0 : -1);
});
