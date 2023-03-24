import fs from 'fs'
import {GoogleSpreadsheet} from 'google-spreadsheet'

const config = require('../config.json');

const serviceAccount = require('../serviceAccount.json');

import {pgclient} from './postgres';

import {google} from "googleapis";

const animalschema = require("../schema.json")

const sheets = google.sheets("v4");

const simpleHash = (str:string) => {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
        const char = str.charCodeAt(i);
        hash = (hash << 5) - hash + char;
        hash &= hash; // Consvert to 32bit integer
    }
    return new Uint32Array([hash])[0].toString(36);
    }

const cors = require("cors")({origin: true});

 // Initialize the sheet - doc ID is the long id in the sheets URL
 const doc = new GoogleSpreadsheet(config.sheetId)
// const { GoogleSpreadsheet } = require('google-spreadsheet');

// Initialize the sheet - doc ID is the long id in the sheets URL
// const doc = new GoogleSpreadsheet('<the sheet ID from the url>');

function cleannumber(input) {
  if (input == null) {
    return null;
  }
  if (input == "") {
    return null;
  } 
  if (input == " ") {
    return null;
  }

  const number = parseInt(input);

  if (!Number.isNaN(number)) {
    return number;
  } else {
    return null;
  }
}

function cleandate(input) {
  if (input == null) {
    return null;
  }
  if (input == "") {
    return null;
  } 
  if (input == " ") {
    return null;
  }
  const darr = input.split("/");

  if (darr.length > 2) {
    
  return `${darr[2]}-${darr[0]}-${darr[1]}`;
  } else {
    return null;
  }
}

function cleanbool(input) {

  if (typeof input === 'string') {
    
  var condensedstring = input.toLowerCase().replace(/\s/g, '').trim();
  if (condensedstring == "yes" || condensedstring == "true") {
    return true;
  } else {
    return false;
  }

} else {
  if (input == true) {
    return true;
  }
  if (input == false) {
    return false;
  }
  if (input == null) {
    return null;
  }

  return null;
}
  
}

async function main() {
    console.log('connecting to postgres...')

  await pgclient.connect()
  console.log('connected to postgres')
 // Initialize Auth - see more available options at https://theoephraim.github.io/node-google-spreadsheet/#/getting-started/authentication
 await doc.useServiceAccountAuth({
   client_email: serviceAccount.client_email,
   private_key: serviceAccount.private_key,
 });

 
async function fetchSheet() {

    await doc.loadInfo();

    //make hash table

    const checkiftableexistsreq = `SELECT EXISTS (
        SELECT FROM 
            information_schema.tables 
        WHERE 
            table_schema LIKE 'public' AND 
            table_type LIKE 'BASE TABLE' AND
            table_name = 'animalhashinfo'
        );`;

    const checkiftableexistsres = await pgclient.query(checkiftableexistsreq);

    if (checkiftableexistsres?.rows[0]?.exists == false) {
    
    const createhashinfo = `CREATE TABLE if not exists animalhashinfo (
        sheetname varchar PRIMARY KEY,
        hash varchar
    );`;

    await pgclient.query(createhashinfo);
    }

    const hashinfoarray = await pgclient.query(`SELECT * FROM animalhashinfo;`);

    const hashinfoexisting = {};

    hashinfoarray.rows.forEach((row) => {
        hashinfoexisting[row.sheetname] = row.hash;
    });

    Object.keys(animalschema.sheets).forEach(async (sheetname) => {
        const sheet = doc.sheetsByTitle[sheetname];

        if (sheet) {
            //sheet exists

            const rows = await sheet.getRows();

            const hash = simpleHash(JSON.stringify(rows.map(x => {
                return x._rawData;
            })));

            let write = true;

            if (hashinfoexisting[sheetname]) {
                if (hashinfoexisting[sheetname] == hash) {
                    write = false;
                }
            }

            if (write) {
                //write to postgres

                const schema = animalschema.sheets[sheetname];

                /* schema looks like this:
                {
                    "month": "varchar",
                    "shelter": "varchar",
                    "amount": "integer",
                    "animal": "varchar"
                }
                */

                const columnforthistable = Object.entries(schema).map((x) => {
                    return `${x[0]} ${x[1]}`
                }).join(', ');

                const columns = Object.keys(schema);

                const createquery = `CREATE TABLE if not exists ${sheetname}new (
                    ${columnforthistable}
                );`;

                await pgclient.query(createquery).then((tablemade) => {
                   //make import statements
               const arrayofinsert:string[] = [];

               rows.forEach((row) => {

                   const valuesarray = columns.map((x) => row[x]);
               
                   arrayofinsert.push(pgclient.query(`INSERT INTO ${sheetname}new (${columns.join(', ')}) VALUES (${valuesarray.map((x, n) => `$${n}`).join(', ')})`
                   , valuesarray));

               });
               
               await Promise.all(arrayofinsert);

               //rename table
               await pgclient.query(`BEGIN; DROP TABLE IF EXISTS ${sheetname}; ALTER TABLE ${sheetname}new RENAME TO ${sheetname}; COMMIT;`);

               //update hash table
               await pgclient.query(`INSERT INTO animalhashinfo (sheetname, hash) VALUES ($1, $2) ON CONFLICT (sheetname) DO UPDATE SET hash = $2;`, [sheetname, hash]);
               
                });
                
              

            }
        }

        
    });

}  

fetchSheet();

//run fetchSheetFunction once every 30 seconds

setInterval(() => {
    fetchSheet();
}, 30000);

}

main();