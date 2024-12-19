const fs = require('fs');
const path = require('path');
const assert = require('assert');
const _ = require('lodash');
const { log } = console;

// Get the first command line argument
const getFirstArgv = () => {
  return String((process.argv || [])[2] || '').trim();
};

const main = () => {
  const pathToCargoFiles = getFirstArgv();
  assert(fs.statSync(pathToCargoFiles).isDirectory(), `path: ${pathToCargoFiles} is not a directory.`);

  const listOfFiles = fs.readdirSync(pathToCargoFiles);

  if (_.isEmpty(listOfFiles)) {
    log(`QUIT no file found in ${pathToCargoFiles}`);
    process.exit();
  }

  for (const filename of listOfFiles) {
    const pathToFile = path.join(pathToCargoFiles, filename);
    log('chking: ', pathToFile);

    const data = fs.readFileSync(pathToFile, { encoding: 'utf8' });
    const lines = data.split(/\r|\n/);

    for (let ln = 0; ln < lines.length; ln++) {
      const line = lines[ln];
      if (!line) {
        continue;
      }

      try {
        const res = JSON.parse(line);
        log(res);
      } catch (error) {
        log(`FOUND INVALID LINE ${ln}, at file:${pathToFile}, content: `, line);
      }
    }
  }
};

main();
