
chalk = require('chalk')
fs = require "fs"
path = require "path"
assert = require "assert"
_ = require "lodash"


{log} = console


# 获取命令行传入的第一个参数
getFirstArgv = ->  String((process.argv || [])[2] || "").trim()


main = ->
  pathToCargoFiles = getFirstArgv()
  assert fs.statSync(pathToCargoFiles).isDirectory(), "path: #{pathToCargoFiles} is not a directory."

  listOfFiles = fs.readdirSync(pathToCargoFiles)
  #log "listOfFiles:", listOfFiles
  if _.isEmpty(listOfFiles)
    log "QUIT no file found in #{pathToCargoFiles}"
    process.exit()

  for filename in listOfFiles
    pathToFile = path.join(pathToCargoFiles, filename)
    log "chking: ", pathToFile
    data = fs.readFileSync(pathToFile, encoding:'utf8')
    lines = data.split(/\r|\n/)
    for line, ln in lines
      #log "line:#{line} ln:#{ln}"
      #line = String(line || '').trim()
      continue unless line
      #try
      res = JSON.parse(line)
      log res
      #catch error
        #log "FOUND INVALID LINE #{ln}, at file:#{pathToFile}, content: ", line

  return



main()

