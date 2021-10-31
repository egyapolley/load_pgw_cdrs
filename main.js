const fs = require("fs")
const path = require("path")
const JSONStream = require('JSONStream')
const moment = require("moment")
const es = require("event-stream")
const mysql = require("mysql2/promise")

const inputDir = path.join(__dirname, "input_dir");



const files = fs.readdirSync(inputDir);

console.log("======================================"+new Date()+"======================================")

if (files.length > 0) {
    let filesIndex =0
    const fileSize = files.length
    let totalRecords =0


    mysql.createConnection({
        host: "localhost",
        database: "sgw_cdrs",
        user: "xxxxx",
        password: "xxxxx"
    }).then((connection) => {
        for (const file of files) {
            try {
                let inputFilePath = path.join(inputDir, file);
                let fileRecords =0;
                const fileStream = fs.createReadStream(inputFilePath, {encoding: 'utf-8'})
                const parser = JSONStream.parse("*.sGWRecord")
                fileStream
                    .pipe(parser)
                    .pipe(es.through(async function (data) {
                            if (data[1].servedIMSI.startsWith("6200802")) {
                                this.pause()
                                await processData(data, this, file, connection)
                                fileRecords++
                            }
                            return data;

                        },
                        function end() {
                            this.emit("end")

                        }
                    ))

                fileStream.on("end", function () {
                    console.log("File processed", file,fileRecords)
                    filesIndex++
                    totalRecords +=fileRecords
                    if (filesIndex === fileSize){
                        connection.end().then(() =>{
                            console.log("DB Connection End")
                            console.log("Total Records Processed:"+totalRecords)
                            console.log("======================================"+new Date()+"======================================")
                            process.exit(0)
                        }).catch(error =>{
                            console.log("Error in Closing Connection",error)
                            console.log("Total Records Processed:"+totalRecords)
                            console.log("======================================"+new Date()+"======================================")
                            process.exit(12)
                        })
                    }

                })


            } catch (e) {
                console.log("Error processing file " + file)
                console.log(e)
            }


        }


    }).catch(error => {
        console.log("DB connection error", error)

    })


}


async function processData(data, es, file, connection,fileRecords) {
    const sGWRecord = data
    if (sGWRecord[1].servedIMSI.startsWith("6200802")) {
        let imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
            duration, causeForRecClosing, recordSequenceNumber, servedPDPPDNAddress,
            msisdn, chargingCharacteristics, userLocationInformation,
            startTime, stopTime, pDNConnectionChargingID, totalVolume = 0

        for (const sGWRecordElement of sGWRecord) {
            if (sGWRecordElement.servedIMSI) imsi = sGWRecordElement.servedIMSI
            if (sGWRecordElement['s-GWAddress']) sGWAddress = sGWRecordElement['s-GWAddress'][0].IPAddress
            if (sGWRecordElement.chargingID) chargingID = sGWRecordElement.chargingID
            if (sGWRecordElement.servingNodeAddress) servingNodeAddress = (sGWRecordElement.servingNodeAddress)[0].IPAddress
            if (sGWRecordElement.servedPDPPDNAddress) servedPDPPDNAddress = (sGWRecordElement.servedPDPPDNAddress)[0].PDPAddress[0].IPAddress
            if (sGWRecordElement.listOfTrafficVolumes) {
                const listOfTrafficData = sGWRecordElement.listOfTrafficVolumes
                for (const listOfTrafficElement of listOfTrafficData) {
                    if (listOfTrafficElement.ChangeOfCharCondition) {
                        const changeOfCharCondition = listOfTrafficElement.ChangeOfCharCondition

                        for (const changeOfCharConditionElement of changeOfCharCondition) {
                            if (changeOfCharConditionElement.datavolumeFBCUplink !== undefined) {
                                totalVolume += changeOfCharConditionElement.datavolumeFBCUplink
                            }

                            if (changeOfCharConditionElement.datavolumeFBCDownlink !== undefined) {
                                totalVolume += changeOfCharConditionElement.datavolumeFBCDownlink
                            }

                        }


                    }


                }


            }
            if (sGWRecordElement.recordOpeningTime) recordOpeningTime = sGWRecordElement.recordOpeningTime
            if (sGWRecordElement.duration !== undefined) duration = sGWRecordElement.duration
            if (sGWRecordElement.causeForRecClosing) causeForRecClosing = sGWRecordElement.causeForRecClosing
            if (sGWRecordElement.recordSequenceNumber) recordSequenceNumber = sGWRecordElement.recordSequenceNumber
            if (sGWRecordElement.servedMSISDN) msisdn = sGWRecordElement.servedMSISDN
            if (sGWRecordElement.chargingCharacteristics) chargingCharacteristics = sGWRecordElement.chargingCharacteristics
            if (sGWRecordElement.userLocationInformation) userLocationInformation = sGWRecordElement.userLocationInformation
            if (sGWRecordElement.startTime) startTime = sGWRecordElement.startTime
            if (sGWRecordElement.stopTime) stopTime = sGWRecordElement.stopTime
            if (sGWRecordElement.pDNConnectionChargingID) pDNConnectionChargingID = sGWRecordElement.pDNConnectionChargingID

            startTime = startTime ? moment(startTime, "YYYY-MM-DD HH:mm:ss Z").format("YYYYMMDDHHmmss") : null
            stopTime = stopTime ? moment(stopTime, "YYYY-MM-DD HH:mm:ss Z").format("YYYYMMDDHHmmss") : null

        }


        /* CDR.create({
             imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
             duration, causeForRecClosing, recordSequenceNumber,
             msisdn, chargingCharacteristics, userLocationInformation,
             startTime, stopTime, totalVolume, pDNConnectionChargingID, servedPDPPDNAddress,
             fileName: file
         }).then(function () {
             es.resume()

         }).catch(error => {
             console.log(error)
             es.resume()
         })*/

        const sql = `insert into cdrs(imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
            duration, causeForRecClosing, recordSequenceNumber,
            msisdn, chargingCharacteristics, userLocationInformation,
            startTime, stopTime, totalVolume, pDNConnectionChargingID, servedPDPPDNAddress,fileName) 
            values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
        try {


            let dataValues = [imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
                duration, causeForRecClosing, recordSequenceNumber,
                msisdn, chargingCharacteristics, userLocationInformation,
                startTime, stopTime, totalVolume, pDNConnectionChargingID, servedPDPPDNAddress, file].map(value => {
                if (value === undefined) return null
                return value
            })
            await connection.execute(sql, dataValues)
            es.resume()


        } catch (ex) {
            console.log(ex)
            es.resume()
        }


    } else {
        es.resume()
    }


}


