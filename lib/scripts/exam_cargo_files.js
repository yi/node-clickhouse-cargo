// Generated by CoffeeScript 2.5.1
(function() {
  var _, assert, chalk, fs, getFirstArgv, log, main, path;

  chalk = require('chalk');

  fs = require("fs");

  path = require("path");

  assert = require("assert");

  _ = require("lodash");

  ({log} = console);

  // 获取命令行传入的第一个参数
  getFirstArgv = function() {
    return String((process.argv || [])[2] || "").trim();
  };

  main = function() {
    var data, filename, i, j, len, len1, line, lines, listOfFiles, ln, pathToCargoFiles, pathToFile, res;
    pathToCargoFiles = getFirstArgv();
    assert(fs.statSync(pathToCargoFiles).isDirectory(), `path: ${pathToCargoFiles} is not a directory.`);
    listOfFiles = fs.readdirSync(pathToCargoFiles);
    //log "listOfFiles:", listOfFiles
    if (_.isEmpty(listOfFiles)) {
      log(`QUIT no file found in ${pathToCargoFiles}`);
      process.exit();
    }
    for (i = 0, len = listOfFiles.length; i < len; i++) {
      filename = listOfFiles[i];
      pathToFile = path.join(pathToCargoFiles, filename);
      log("chking: ", pathToFile);
      data = fs.readFileSync(pathToFile, {
        encoding: 'utf8'
      });
      lines = data.split(/\r|\n/);
      for (ln = j = 0, len1 = lines.length; j < len1; ln = ++j) {
        line = lines[ln];
        if (!line) {
          //log "line:#{line} ln:#{ln}"
          //line = String(line || '').trim()
          continue;
        }
        //try
        res = JSON.parse(line);
        log(res);
      }
    }
  };

  //catch error
  //log "FOUND INVALID LINE #{ln}, at file:#{pathToFile}, content: ", line
  main();

}).call(this);
