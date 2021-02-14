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

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the functionality to a hidden input connected to the controller
function goToPageAfterCreate(functionality) {

    document.getElementById("functionalitySelected4").value = functionality;

    var data;
    var array = a = document.querySelectorAll("[id$=DropdownText]");
    for (i = 0; i < array.length; i++) {
        if (array[i].innerHTML != 'None') {
            data += '/';
            data += array[i].id.replace("DropdownText", '');
            data += ',';
            data += array[i].innerHTML;
        }
    }

    document.getElementById("data").value = data;

    $("#hidden-btn5").click();
}

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the functionality to a hidden input connected to the controller
function goToPageAll(functionality) {
    if (functionality == 'data_masking') {
        functionality = 'create_masks';
    }
    if (functionality == 'constraints') {
        functionality = 'create_constraints';
    }
    document.getElementById("functionalitySelected2").value = functionality;
    $("#hidden-btn3").click();
}

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the functionality to a hidden input connected to the controller
function goToPage(functionality) {

    if (getColumns() == 0 && getTables() == 0) {
        alert('You have to select at least one column');
    } else {

        if (functionality == 'data_masking') {
            functionality = 'create_masks';
        }
        if (functionality == 'constraints') {
            functionality = 'create_constraints';
        }

        var selected;

        document.querySelectorAll('input[type=checkbox][id$=CheckBox]').forEach(
            function (item) {
                if (item.checked) {
                    selected += "/";
                    selected += item.name;

                    document.querySelectorAll('[ data-parent=' + item.id + ']').forEach(
                        function (column) {
                            if (column.checked) {
                                selected += ",";
                                selected += column.id;
                            }
                        });
                }
            });

        document.getElementById("functionalitySelected3").value = functionality;
        document.getElementById("selection").value = selected;
        $("#hidden-btn4").click();

    }

}

// this method parameter is the id of the table checkbox
// this method is called when the table checkbox is clicked
// first checks if that checkbox is checked
// then applies that value to all columns checkbox of that table and updates the output text
function checkChilds(CheckBoxparent) {
    var boo = document.getElementById(CheckBoxparent).checked;
    document.querySelectorAll('[ data-parent=' + CheckBoxparent + ']').forEach(
        function (item) {
            item.checked = boo;
        });
    if (getColumns() == 0) {
        if (getTables() != 0) {
            document.getElementById('selection-text').innerHTML = getTables() + ' tables selected'
        } else {
            document.getElementById('selection-text').innerHTML = 'None selected'
        }
    } else {
        document.getElementById('selection-text').innerHTML = getColumns() + ' columns selected from ' + getTables() + ' different tables.'
    }
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
    if (getColumns() == 0) {
        if (getTables() != 0) {
            document.getElementById('selection-text').innerHTML = getTables() + ' tables selected'
        } else {
            document.getElementById('selection-text').innerHTML = 'None selected'
        }
    } else {
        document.getElementById('selection-text').innerHTML = getColumns() + ' columns selected from ' + getTables() + ' different tables.'
    }
}

// this method parameter checks all the checkboxes and updates the output text
function selectAll() {
    document.querySelectorAll('input[type=checkbox]').forEach(
        function (item) {
            item.checked = true;
        });
    if (getColumns() == 0) {
        document.getElementById('selection-text').innerHTML = getTables() + ' tables selected'
    } else {
        document.getElementById('selection-text').innerHTML = getColumns() + ' columns selected from ' + getTables() + ' different tables.'
    }
}

// this method parameter unchecks all the checkboxes and updates the output text
function selectNone() {
    document.querySelectorAll('input[type=checkbox]').forEach(
        function (item) {
            item.checked = false;
        });
    document.getElementById('selection-text').innerHTML = 'None selected'
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
    document.getElementById(name + 'Dropdown').style.display = "inline";
    event.currentTarget.className += " active";
}
// This function more or less lets you download the page in pdf format
function download() {
    var doc = new jsPDF();
    var specialElementHandlers = {
        '#editor': function (element, renderer) {
            return true;
        }
    };
    doc.fromHTML($('main').html(), 15, 15, {
        'width': 170,
        'elementHandlers': specialElementHandlers
    });
    doc.save('sample-file.pdf');
}

// For Dropdown menus

function changeName(dropdown, text) {
    document.getElementById(dropdown).textContent = text;
}