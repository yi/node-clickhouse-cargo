{createCargo, isInited} = require "../"
assert = require ("assert")
fs = require "fs"

QUERY = "INSERT INTO test.cargo0 FORMAT JSONCompactEachRow"

describe "push log to cargo", ->
  @timeout(5000)

  theCargo = null
  theBulk = null
  theFilepath = null

  before (done)->
    theCargo = createCargo(QUERY, 999000)
    theBulk = theCargo.curBulk
    theFilepath = theBulk.pathToFile

    done()
    return


  after (done)->
    process.exit()
    done()
    return

  it "push to cargo", (done)->

    for i in [0...100]
      theCargo.push new Date, i, "string"

    assert fs.existsSync(theFilepath), "log file not exist on #{theFilepath}"
    setTimeout(done, 2000) # wait file stream flush
    return

  it "exam content written on hd file", (done)->
    contentInHD = fs.readFileSync(theFilepath, 'utf8')
    contentInHDArr =  contentInHD.split(/\r|\n|\r\n/)

    console.log "[exam hd content] contentInHDArr:", contentInHDArr.length
    assert contentInHDArr.length is 100, "unmatching output length"

    for line, i in contentInHDArr
      line = JSON.parse(line)
      #console.log line
      assert line[1] is i, "unmatching field 1 "
      assert line[2] is "string" , "unmatching field 2 "

    done()
    return


