csv = require 'csv-parse'
fs = require 'fs'
kd = require 'kdtree'
WebSocketServer = require('ws').Server

company_tree = new kd.KDTree(2)
service_tree = new kd.KDTree(2)

listen_port = 8374

# populate service tree
for i in [1 .. 1000]
  service_tree.insert(Math.random() * 360.0 - 180.0, Math.random() * 360.0 - 180.0, i)

handle_socket = (ws) ->
  ws.on 'message', (m) ->
    try
      json = JSON.parse m
    catch e
      console.log "Received malformed message '#{m}'"
      return
    if json["type"] is "company_query"
      center = json["center"]
      radius = json["radius"]
      ret = company_tree.nearestRange center[0], center[1], radius
      ws.send JSON.stringify ret
    else if json["type"] is "service_query"
      center = json["location"]
      ret = service_tree.nearest center[0], center[1]
      ws.send JSON.stringify ret

start_server = ->
  wss = new WebSocketServer port: listen_port
  wss.broadcast = (d) -> wss.clients.forEach (c) -> c.send d
  wss.on 'connection', (ws) -> handle_socket ws
  console.log "Listening for connections on port #{listen_port}"

error = (m) ->
  console.log m
  process.exit 1

# Read in company data
parser = csv delimiter: ','

parser.on 'readable', ->
  while record = parser.read()
    error "Malformed record of length #{record.length}" unless record.length is 3
    company_tree.insert parseFloat(record[1]), parseFloat(record[2]), record[0]

parser.on 'error', (err) -> error err.message
parser.on 'finish', start_server

parser.write fs.readFileSync('filtered-companies.csv', 'utf8')
parser.end()