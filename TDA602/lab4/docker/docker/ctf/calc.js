const vm = require("vm");

function evalInJail(env, options, script) {
    const context = vm.createContext(env);
    const src = `"use strict";\n${script}`;

    return vm.createScript(src).runInNewContext(context, options);
}

function main(line) {
    const env = {};

    const options = {
        timeout: 1000,
    };

    const whitelist = /^(\s*(Math\.min|Math\.max|[0-9]+|[\(\)\[\],\*\/\+\-])\s*)+$/i;

    var res = "";
    try {
        if (!whitelist.test(line)) {
            throw "devil attempts!";
        }
        res = evalInJail(env, options, line);
    } catch (e) {
        res = "invalid input!";
    }

    return res;
}

process.on("uncaughtException", function(err) {
    console.log("...");
});

module.exports = main;
