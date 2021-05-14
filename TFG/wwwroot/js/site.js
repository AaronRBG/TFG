// it was used to open a dialog to choose a database .mdf file currently in disuse
function openfileDialog() {
    $("#fileLoader").click();
}

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the texts from a textarea to a hidden input connected to the controller
function connect() {
    document.getElementById("connectionString").value = document.getElementById("constr").value
    $("#hidden-btn").click();
}

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the functionality to a hidden input connected to the controller
function goToSelection(functionality) {
    if (functionality == 'data_masking') {
        functionality = 'create_masks';
    }
    if (functionality == 'constraints') {
        functionality = 'create_constraints';
    }
    document.getElementById("functionalitySelected1").value = functionality;
    $("#hidden-btn1").click();
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
    $("#hidden-btn2").click();
}

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the functionality to a hidden input connected to the controller
function goToPage(functionality, page) {

    if (page && getColumns() == 0 && getTables() == 0) {
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

        if (page) {
            document.getElementById("selection").value = selected;
        }
        $("#hidden-btn3").click();

    }
}

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the functionality to a hidden input connected to the controller
function goToPageAfterCreate(functionality) {

    if (functionality == 'create_masks') {
        functionality = 'data_masking';
    }
    if (functionality == 'create_constraints') {
        functionality = 'constraints';
    }

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

    if (data == undefined) {
        alert('You have to select at least one mask or constraint');
    } else {

        document.getElementById("data").value = data;

        $("#hidden-btn4").click();
    }
}

// used as a middleware to activate a hidden button which is the one who connects to the controller and also inputs the functionality to a hidden input connected to the controller
function confirm(functionality) {
    if (getRecords() == 0 && getColumns() == 0 && getTables() == 0) {
        alert('You have to select at least one row');
    }
    else if ((functionality.includes("Rows") && getRecords() == 0) || ((functionality.includes("Columns") && getColumns() == 0))) {
        alert('You have to select at least one correct element');
    } else {

        var selected;
        var aux;
        if (functionality.includes("missing_values")) {
            selected = functionality.replace("missing_values", '');
        }

        if (functionality == "create_masks" || functionality == "remove_duplicates" || functionality == "missing_valuesupdateRows" || functionality == "missing_valuesdeleteRows") {

            document.querySelectorAll('input[type=checkbox][id$=Record]').forEach(
                function (item) {
                    if (item.checked) {

                        if (aux == undefined || item.attributes["data-parent"].value.replace("CheckBox", '') != aux) {
                            selected += "/";
                            aux = item.attributes["data-parent"].value.replace("CheckBox", '');
                            selected += aux;
                        }
                        selected += ',';
                        var temp;
                        if (!aux.includes('.')) {
                            temp = aux;
                        } else {
                            temp = aux.split('.')[1];
                        }
                        selected += (item.id.replace(temp, '').replace('Record', ''));

                        if (functionality == "missing_valuesupdateRows") {
                            item.parentElement.parentElement.childNodes.forEach(function (inputNode) {
                                var input = inputNode.childNodes[0];
                                if (input != undefined && input.className == 'form-control' && input.value != '') {
                                    selected += '_' + input.id.split("Record")[1] + '=' + input.value;
                                }
                            })
                        }
                    }
                });
        }

        if (functionality == "primary_keys") {
            document.querySelectorAll('input[type=checkbox][id$=CheckBox]').forEach(
                function (item) {
                    if (item.checked) {
                        selected += ",";
                        selected += item.name;
                    }
                });
        }

        if (functionality == "improve_datatypes" || functionality == "missing_valuesdeleteColumns") {
            if (functionality == "missing_valuesdeleteColumns") {
                selected += '/undefined';
            }
            document.querySelectorAll('input[type=checkbox]:not([id$=CheckBox]):not([id$=Record])').forEach(
                function (item) {
                    if (item.checked) {
                        selected += ",";
                        selected += item.name;
                    }
                });
        }

        document.getElementById("data").value = selected;
        document.getElementById("functionalitySelected5").value = functionality;
        $("#hidden-btn5").click();
    }

}

// used to update the selection text field
function updateSelectionText(name) {
    document.getElementById('records-selected').innerHTML = ''
    document.getElementById('columns-selected').innerHTML = ''
    document.getElementById('tables-selected').innerHTML = ''
    if (getRecords() == 0) {
        if (getColumns() == 0) {
            if (getTables() != 0) {
                document.getElementById('tables-selected').innerHTML = getTables() + ' tables selected'
            } else {
                document.getElementById('columns-selected').innerHTML = 'None selected'
            }
        } else {
            document.getElementById('columns-selected').innerHTML = getColumns() + ' columns selected';
            document.getElementById('tables-selected').innerHTML = 'from ' + getTables() + ' different tables.'
        }
    } else {
        document.getElementById('records-selected').innerHTML = getRecords() + ' records selected';
        if (getColumns() != 0) {
            document.getElementById('columns-selected').innerHTML = 'from ' + getColumns() + ' different columns';
        }
        document.getElementById('tables-selected').innerHTML = 'and ' + getTables() + ' different tables.';
    }
}

// this method parameter is the id of the table checkbox
// this method is called when the table checkbox is clicked
// first checks if that checkbox is checked
// then applies that value to all columns checkbox of that table and updates the output text
function checkChilds(CheckBoxparent) {
    var boo = document.getElementById(CheckBoxparent).checked;
    document.querySelectorAll('[ data-parent=' + '"' + CheckBoxparent + '"' + ']').forEach(
        function (item) {
            item.checked = boo;
            checkChilds(item.id);
        });
    updateSelectionText();
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
    document.querySelectorAll('[ data-parent=' + '"' + CheckBoxparent + '"' + ']').forEach(
        function (item) {
            if (item.checked) {
                boo = true;
            }
        });
    if (!boo) {
        document.getElementById(CheckBoxparent).checked = boo;
    }
    updateSelectionText();
    checkParent(document.getElementById(CheckBoxparent).attributes["data-parent"].value, CheckBoxparent);
}

// this method is used for checkboxes who have both parent checkboxes and child checkboxes
function checkBoth(CheckBoxparent, current) {
    /*
    checkChilds(current);
    checkParent(CheckBoxparent, current);
    */
}

// this method parameter checks all the checkboxes and updates the output text
function selectAll() {
    document.querySelectorAll('input[type=checkbox]').forEach(
        function (item) {
            item.checked = true;
        });
    updateSelectionText();
}

// this method parameter checks all the column checkboxes and updates the output text
function selectColumns() {
    document.querySelectorAll('input[type=checkbox]:not([id$=CheckBox]):not([id$=Record])').forEach(
        function (item) {
            if (item.parentElement.parentElement.parentElement.parentElement.parentElement.style.display != "none") {
                item.checked = true;
            }
        });
    var aux;
    document.querySelectorAll('div[class=tabcontent]').forEach(
        function (item) {
            if (item.style.display != "none") {
                aux = item.id.replace('Div', '') + 'CheckBox';
            }
        });
    document.querySelectorAll('input[type=checkbox][id=' + aux + ']').forEach(
        function (item) {
            item.checked = true;
        });
    updateSelectionText();
}

// this method parameter unchecks all the column checkboxes and updates the output text
function unselectColumns() {
    document.querySelectorAll('input[type=checkbox]:not([id$=CheckBox]):not([id$=Record])').forEach(
        function (item) {
            if (item.parentElement.parentElement.parentElement.parentElement.parentElement.style.display != "none") {
                item.checked = false;
            }
        });
    var aux;
    document.querySelectorAll('div[class=tabcontent]').forEach(
        function (item) {
            if (item.style.display != "none") {
                aux = item.id.replace('Div', '') + 'CheckBox';
            }
        });
    boo = true;
    document.querySelectorAll('input[type=checkbox][name*="' + aux.replace('CheckBox', '') + '."]:not([id$=CheckBox])').forEach(
        function (item) {
            if (item.checked) {
                boo = false;
            }
        });
    document.querySelectorAll('input[type=checkbox][id=' + aux + ']').forEach(
        function (item) {
            if (boo) {
                item.checked = false;
            }
        });
    updateSelectionText();
}

// this method parameter checks all the record checkboxes and updates the output text
function selectRecords(fromColumn) {
    document.querySelectorAll('input[type=checkbox][id$=Record]').forEach(
        function (item) {
            if (item.parentElement.parentElement.parentElement.parentElement.parentElement.style.display != "none") {
                item.checked = true;
            }
        });
    var aux;
    document.querySelectorAll('div[class=tabcontent]').forEach(
        function (item) {
            if (item.style.display != "none") {
                aux = item.id.replace('Div', '') + 'CheckBox';
            }
        });
    var par;
    var element;
    if (fromColumn) {
        aux = aux.replace('CheckBox', '');
        par = 'name="' + aux + '"';
    } else {
        par = 'id=' + aux;
    }
    document.querySelectorAll('input[type=checkbox][' + par + ']').forEach(
        function (item) {
            item.checked = true;
            element = item;
        });
    if (fromColumn) {
        par = 'id=' + element.attributes["data-parent"].textContent;
        document.querySelectorAll('input[type=checkbox][' + par + ']').forEach(
            function (item) {
                    item.checked = true;
            });
    }
    updateSelectionText();
}

// this method parameter unchecks all the record checkboxes and updates the output text
function unselectRecords(fromColumn) {
    document.querySelectorAll('input[type=checkbox][id$=Record]').forEach(
        function (item) {
            if (item.parentElement.parentElement.parentElement.parentElement.parentElement.style.display != "none") {
                item.checked = false;
            }
        });
    var aux;
    document.querySelectorAll('div[class=tabcontent]').forEach(
        function (item) {
            if (item.style.display != "none") {
                aux = item.id.replace('Div', '') + 'CheckBox';
            }
        });
    boo = true;
    if (!fromColumn) {
        document.querySelectorAll('input[type=checkbox][name*="' + aux.replace('CheckBox', '') + '"]:not([id$=CheckBox])').forEach(
            function (item) {
                if (item.checked) {
                    boo = false;
                }
            });
    }
    var par;
    var element;
    if (fromColumn) {
        aux = aux.replace('CheckBox', '');
        par = 'name="' + aux + '"';
    } else {
        par = 'id=' + aux;
    }
    document.querySelectorAll('input[type=checkbox][' + par + ']').forEach(
        function (item) {
            element = item;
            if (boo) {
                item.checked = false;
            }
        });
    if (fromColumn && getTableColumns(aux.split('.')[0]) == 0) {
        par = 'id=' + element.attributes["data-parent"].textContent;
        document.querySelectorAll('input[type=checkbox][' + par + ']').forEach(
            function (item) {
                if (boo) {
                    item.checked = false;
                }
            });
    }
    updateSelectionText();
}

// this method parameter unchecks all the checkboxes and updates the output text
function selectNone() {
    document.querySelectorAll('input[type=checkbox]').forEach(
        function (item) {
            item.checked = false;
        });
    updateSelectionText();
}

// this method parameter returns the number of table checkboxes that are checked
function getTables() {
    checked = 0;
    document.querySelectorAll('input[type=checkbox][id$=CheckBox]').forEach(
        function (item) {
            if (item.checked) {
                checked++;
            }
        });
    return checked;
}

// this method parameter returns the number of column checkboxes that are checked
function getColumns() {
    checked = 0;
    document.querySelectorAll('input[type=checkbox]:not([id$=CheckBox]):not([id$=Record])').forEach(
        function (item) {
            if (item.checked) {
                checked++;
            }
        });
    return checked;
}

// this method parameter returns the number of column checkboxes that are checked and belong to the given table
function getTableColumns(table) {
    checked = 0;
    document.querySelectorAll('input[type=checkbox][name*="' + table + '."]:not([id$=CheckBox]):not([id$=Record])').forEach(
        function (item) {
            if (item.checked) {
                checked++;
            }
        });
    return checked;
}

// this method parameter returns the number of record checkboxes that are checked
function getRecords() {
    checked = 0;
    document.querySelectorAll('input[type=checkbox][id$=Record]').forEach(
        function (item) {
            if (item.checked) {
                checked++;
            }
        });
    return checked;
}

function checkRecord(event, name, functionality) {

    document.getElementById("record").value = name;
    document.getElementById("functionalitySelected6").value = functionality;

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

    document.getElementById("data2").value = data;

    var aux;

    document.querySelectorAll("[id^=collapse]").forEach(function (item) {
        if (item.className.includes("show")) {
            aux = item.id;
        }
    });

    document.getElementById("accordionInfo").value = aux;

    $("#hidden-btn6").click();

}

function checkMissingValue(event, name, functionality) {

    document.getElementById("missingValue").value = name;
    document.getElementById("functionalitySelected7").value = functionality;
    $("#hidden-btn7").click();

}

// This function controls the vertical tabs in some views like reports
function openTab(event, name, create) {
    // Declare all variables
    var i, tabcontent, tablinks;

    // Get all elements with class="tabcontent" and hide them
    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }

    // Show the current tab, and add an "active" class to the link that opened the tab
    document.getElementById(name).style.display = "block";
    if (create) {
        document.getElementById(name + 'Dropdown').style.display = "inline";
    }
    event.currentTarget.className += " active";
}
// This function more or less lets you download the page in pdf format
function downloadCSV(csv, filename) {
    var csvFile;
    var downloadLink;

    // CSV file
    csvFile = new Blob([csv], { type: "text/csv" });

    // Download link
    downloadLink = document.createElement("a");

    // File name
    downloadLink.download = filename;

    // Create a link to the file
    downloadLink.href = window.URL.createObjectURL(csvFile);

    // Hide download link
    downloadLink.style.display = "none";

    // Add the link to DOM
    document.body.appendChild(downloadLink);

    // Click download link
    downloadLink.click();
}

function exportTableToCSV(filename) {
    var csv = [];
    var rows = document.querySelectorAll("table tr");

    for (var i = 0; i < rows.length; i++) {
        var row = [], cols = rows[i].querySelectorAll("td, th");

        for (var j = 1; j < cols.length; j++)
            row.push(cols[j].innerText);

        csv.push(row.join(","));
    }

    // Download CSV file
    downloadCSV(csv.join("\n"), filename);
}

// For Dropdown menus
function changeName(dropdown, text) {
    document.getElementById(dropdown).textContent = text;
}
