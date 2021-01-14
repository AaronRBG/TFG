// Please see documentation at https://docs.microsoft.com/aspnet/core/client-side/bundling-and-minification
// for details on configuring this project to bundle and minify static web assets.

// Write your JavaScript code.

// it was used to open a dialog to choose a database .mdf file currently in disuse
function openfileDialog() {
    $("#fileLoader").click();
}

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the texts from a textarea to a hidden input connected to the controller
function connect() {
    document.getElementById("connectionString").value = document.getElementById("constr").value
    $("#hidden-btn1").click();
}

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the functionality to a hidden input connected to the controller
function goToSelection(functionality) {
    document.getElementById("functionalitySelected").value = functionality;
    $("#hidden-btn2").click();
}

// this method parameter is the id of the table checkbox
// this method is called when the table checkbox is clicked
// first checks if that checkbox is checked
// then applies that value to all columns checkbox of that table and updates the output text
function checkChilds(CheckBoxparent) {
    var boo = document.getElementById(CheckBoxparent).checked;
    document.querySelectorAll('[ data-parent='+CheckBoxparent+']').forEach(
        function (item) {
            item.checked = boo;
        });
    document.getElementById('selection-text').innerHTML = getColumns() + ' columns selected from ' + getTables() + ' different tables.'
}

// this method parameter is the id of the table checkbox and the id of the column checkbox clicked
// this method is called when a column checkbox is clicked
// first checks if that checkbox is checked
// if it is checked it checks the table checkbox of table it belong to
// then checks that if all columns checkbox of a table are not checked it unchecks the table checkbox and updates the output text
function checkParent(CheckBoxparent, child) {
    var boo = document.getElementById(child).checked;
    if (boo) {
        document.getElementById(CheckBoxparent).checked = boo;
    }
    boo = false;
    document.querySelectorAll('[ data-parent=' + CheckBoxparent + ']').forEach(
        function (item) {
            if (item.checked) {
                boo = true;
            }
        });
    if (!boo) {
        document.getElementById(CheckBoxparent).checked = boo;
    }
    document.getElementById('selection-text').innerHTML = getColumns() + ' columns selected from ' + getTables() + ' different tables.'
}

// this method parameter checks all the checkboxes and updates the output text
function selectAll() {
    document.querySelectorAll('input[type=checkbox]').forEach(
        function (item) {
            item.checked = true;
        });
    document.getElementById('selection-text').innerHTML = getColumns() + ' columns selected from ' + getTables() + ' different tables.'
}

// this method parameter unchecks all the checkboxes and updates the output text
function selectNone() {
    document.querySelectorAll('input[type=checkbox]').forEach(
        function (item) {
            item.checked = false;
        });
    document.getElementById('selection-text').innerHTML = 'None Selected'
}

// this method parameter returns the number of table checkboxes that are clicked
function getTables() {
    checked = 0.0;
    document.querySelectorAll('input[type=checkbox][id$=CheckBox]').forEach(
        function (item) {
            if (item.checked) {
                checked++;
            }
        });
    return checked;
}

// this method parameter returns the number of column checkboxes that are clicked
function getColumns() {
    checked = 0.0;
    document.querySelectorAll('input[type=checkbox]:not([id$=CheckBox])').forEach(
        function (item) {
            if (item.checked) {
                checked++;
            }
        });
    return checked;
}

//
function goToPage(functionality) {
    window.location.href = functionality;
}

// This function controls the vertical tabs in some views like reports
function openTab(event, name) {
    // Declare all variables
    var i, tabcontent, tablinks;

    // Get all elements with class="tabcontent" and hide them
    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }

    // Show the current tab, and add an "active" class to the link that opened the tab
    document.getElementById(name).style.display = "block";
    event.currentTarget.className += " active";
} 