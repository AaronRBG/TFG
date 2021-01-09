// Please see documentation at https://docs.microsoft.com/aspnet/core/client-side/bundling-and-minification
// for details on configuring this project to bundle and minify static web assets.

// Write your JavaScript code.

function openfileDialog() {
    $("#fileLoader").click();
}

function connect() {
    document.getElementById("connectionString").value = document.getElementById("constr").value
    $("#hidden-btn1").click();
}

function goToSelection(functionality) {
    document.getElementById("functionalitySelected").value = functionality;
    $("#hidden-btn2").click();
}

function checkChilds(CheckBoxparent) {
    var boo = document.getElementById(CheckBoxparent).checked;
    document.querySelectorAll('[ data-parent='+CheckBoxparent+']').forEach(
        function (item) {
            item.checked = boo;
        });
}

function selectAll() {
    document.querySelectorAll('input[type=checkbox]').forEach(
        function (item) {
            item.checked = true;
        });
}

function selectNone() {
    document.querySelectorAll('input[type=checkbox]').forEach(
        function (item) {
            item.checked = false;
        });
}

function getPercent() {
    checked = 0.0;
    total = 0.0;
    document.querySelectorAll('input[type=checkbox]').forEach(
        function (item) {
            total++;
            if (item.checked) {
                checked++;
            }
        });
    return (checked*100)/total;
}
