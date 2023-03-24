import fs from 'fs'

var argv = require('optimist').argv;

const config = require('../config.json');

  var pgclientconfig = {
    user: config.db.username,
    host: config.db.host,
    database: config.db.database,
    password: config.db.password,
    port: config.db.port,
    
  }

if (argv.nossl) {

} else {
  pgclientconfig['ssl'] = {
      rejectUnauthorized: false,
      ca: fs.readFileSync('./server-ca.pem').toString(),
      key: fs.readFileSync('./client-key.pem').toString(),
      cert:  fs.readFileSync('./client-cert.pem').toString()

      }    }



const { Client } = require('pg')
export const pgclient = new Client(pgclientconfig)