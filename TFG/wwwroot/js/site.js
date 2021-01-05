// Please see documentation at https://docs.microsoft.com/aspnet/core/client-side/bundling-and-minification
// for details on configuring this project to bundle and minify static web assets.

// Write your JavaScript code.

function openfileDialog() {
    $("#fileLoader").click();
}

function connect() {
    document.getElementById("connectionString").value = document.getElementById("constr").value
    $("#hidden-btn").click();
}